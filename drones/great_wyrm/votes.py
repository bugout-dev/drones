"""
Parse votes from Humbug journal for Great Wyrm, 
generate statistics and upload to S3 bucket.

[
    {
        game_session_id: <uuid>
        player_id: <uuid> (store in in SessionStorage at web browser)
        stage: <int>
        path: <int>
        created_at: <datetime> (from entry)
    }
]

Structure:
- game_session
    - stage 1
        - paths
    - stage 2

Two times of json stats:
1. List of game sessions with stages - show total number of votes
2. List of votes for specific game session -> stage - showing votes for different paths

(1) - For menu
1. Fetch all votes
2. Combine all votes depends on game_session and stage
3. Remove same temp_user_id for same path
4. Show list of stages with total votes number

(2) - For dashboard
1. Fetch all votes with "tag:game_session:<uuid>&tag:stage:<int>"
2. Show votes for each path
"""

import argparse
import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime
from pprint import pprint
from typing import Any, Dict, List
from uuid import UUID

import boto3
from pydantic import BaseModel, Field
from spire.db import yield_db_read_only_session
from spire.journal.models import JournalEntry, JournalEntryTag
from sqlalchemy import ARRAY, String, and_, func, text
from sqlalchemy.orm import Query, Session, aliased
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import cast

from ..settings import (
    MOONSTREAM_S3_PUBLIC_DATA_BUCKET,
    MOONSTREAM_S3_PUBLIC_DATA_BUCKET_PREFIX,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")

yield_db_read_only_session_ctx = contextmanager(yield_db_read_only_session)

DEFAULT_VOTES_CHUNK_SIZE = 100


class Vote(BaseModel):
    player_id: UUID
    created_at: datetime

    path: int
    stage: int
    game_session_id: UUID


class GameStagePath(BaseModel):
    path: int
    votes: List[Vote] = Field(default_factory=list)


class GameStage(BaseModel):
    stage: int
    paths: List[GameStagePath] = Field(default_factory=list)


class Game(BaseModel):
    game_session_id: UUID
    stages: List[GameStage] = Field(default_factory=list)


def push_stats(data: Dict[str, Any], filename: str) -> None:
    """
    Push data to bucket.
    """
    if MOONSTREAM_S3_PUBLIC_DATA_BUCKET is None:
        logger.warning(
            "MOONSTREAM_S3_PUBLIC_DATA_BUCKET environment variable not set, skipping data upload"
        )
        return

    result_bytes = json.dumps(data).encode("utf-8")
    result_key = (
        f"{MOONSTREAM_S3_PUBLIC_DATA_BUCKET_PREFIX}/great_wyrm/votes/{filename}"
    )

    try:
        s3.put_object(
            Body=result_bytes,
            Bucket=MOONSTREAM_S3_PUBLIC_DATA_BUCKET,
            Key=result_key,
            ContentType="application/json",
            Metadata={"greatwyrm": "votes"},
        )

        logger.info(
            f"Great Wyrm vote stats pushed to bucket: s3://{MOONSTREAM_S3_PUBLIC_DATA_BUCKET}/{result_key}"
        )
    except Exception:
        logger.error(
            f"Failed to push data to bucket: s3://{MOONSTREAM_S3_PUBLIC_DATA_BUCKET}/{result_key}"
        )


def fetch_game_sessions_votes(db_session: Session, journal_id: UUID) -> Query:
    """
    Query database for list of game sessions and number of votes in it.
    """
    len_game_session_id_str = len("game_session_id:") + 1

    # Game Session IDs
    entry_ids_game_session_ids = (
        db_session.query(
            JournalEntryTag.journal_entry_id.label("entry_id"),
            func.substr(JournalEntryTag.tag, len_game_session_id_str).label(
                "game_session_id"
            ),
        )
        .join(JournalEntry, JournalEntry.id == JournalEntryTag.journal_entry_id)
        .filter(JournalEntry.journal_id == journal_id)
        .filter(JournalEntryTag.tag.like("game_session_id:%"))
    ).subquery(name="entry_ids_game_session_ids")

    # Group by session id (tag)
    game_sessions_votes = (
        db_session.query(
            entry_ids_game_session_ids.c.game_session_id.label("game_session_id"),
            func.count(entry_ids_game_session_ids.c.entry_id).label(
                "game_session_votes"
            ),
        )
        .group_by(entry_ids_game_session_ids.c.game_session_id)
        .order_by(text("game_session_votes DESC"))
    )

    return game_sessions_votes


def fetch_votes(db_session: Session, journal_id: UUID):
    """
    Fetch all votes.
    """
    game_session_tag = "game_session_id"
    stage_tag = "stage"
    path_tag = "path"
    player_id_tag = "player_id"

    result = (
        db_session.execute(  # type: ignore
            text(
                """
    With events_table as (
        SELECT
            journal_entry_id,
                CASE 
                    WHEN tag LIKE :game_session_tag || ':%' THEN substr(tag, POSITION(':' in tag) + 1)
                    ELSE NULL
                END as game_session_id,
                CASE 
                    WHEN tag LIKE :stage_tag || ':%' THEN substr(tag, POSITION(':' in tag) + 1)
                    ELSE NULL
                END as stage,
                CASE 
                    WHEN tag LIKE :path_tag || ':%' THEN substr(tag, POSITION(':' in tag) + 1)
                    ELSE NULL
                END as path,
                CASE 
                    WHEN tag LIKE :player_id_tag || ':%' THEN substr(tag, POSITION(':' in tag) + 1)
                    ELSE NULL
                END as player_id,
                to_char(journal_entries.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created_at
        FROM journal_entry_tags JOIN journal_entries on journal_entry_tags.journal_entry_id = journal_entries.id
        WHERE journal_id = :journal_id
    ), grouping as (
        SELECT DISTINCT
            journal_entry_id,
            string_agg(game_session_id,',') as game_session_id,
            string_agg(stage,',') as stage,
            string_agg(path,',') as path,
            string_agg(player_id,',') as player_id,
            created_at
        FROM events_table
        GROUP BY 1,6
    ), paths as (SELECT
            game_session_id,
            stage,
            json_agg(json_build_object(
            :path_tag, path,
            :player_id_tag, player_id,
            'created_at', created_at
            )) as path_player_ids
        FROM grouping
        GROUP BY 1, 2
    ), stages as (SELECT
            game_session_id,
            json_agg(json_build_object(
            :stage_tag, stage, 
            'paths', path_player_ids
            )) as stage_path_player_ids
        FROM paths
        GROUP BY 1
    )SELECT
        json_agg(json_build_object(
        :game_session_tag,  game_session_id,
        'stages', stage_path_player_ids
        )) as json_data
        FROM stages;
    """
            ),
            {
                "game_session_tag": game_session_tag,
                "stage_tag": stage_tag,
                "path_tag": path_tag,
                "player_id_tag": player_id_tag,
                "journal_id": str(journal_id),
            },
        )
        .fetchone()
        .json_data
    )

    return result


def stats_update_handler(args: argparse.Namespace) -> None:
    with yield_db_read_only_session_ctx() as db_session:
        game_sessions_votes_query = fetch_game_sessions_votes(
            db_session=db_session, journal_id=args.journal
        )
        print(game_sessions_votes_query.all())

        votes_query = fetch_votes(db_session=db_session, journal_id=args.journal)

        data = {}
        for vote in votes_query:
            print(vote)
            # data[i[0]] = i[1]

        if args.push_to_bucket:
            push_stats(data=data, filename="game_session_ids.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Great Wyrm humbug votes CLI")
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(description="Great Wyrm commands")

    # Stats parser
    parser_stats = subcommands.add_parser("stats", description="Stats commands")
    parser_stats.set_defaults(func=lambda _: parser_stats.print_help())
    subcommands_stats = parser_stats.add_subparsers(description="Stats commands")

    parser_stats_update = subcommands_stats.add_parser(
        "update", description="Update stats of Great Wyrm humbug votes"
    )
    parser_stats_update.add_argument(
        "-j",
        "--journal",
        required=True,
        help=f"Humbug journal ID with Great Wyrm votes",
    )
    parser_stats_update.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_VOTES_CHUNK_SIZE,
        help="Size of processing votes at one moment",
    )
    parser_stats_update.add_argument(
        "--push-to-bucket",
        action="store_true",
        help="Push to AWS S3 bucket if argument set",
    )

    parser_stats_update.set_defaults(func=stats_update_handler)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
