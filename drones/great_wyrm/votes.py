"""
Parse votes from Humbug journal for Great Wyrm, 
generate statistics and upload to S3 bucket.

Structure:

[
    game_session_id: <uuid>,
    stages: [
        stage: <int>,
        paths: [
            path: <int>,
            client_id: <uuid>,  # store in in SessionStorage at web browser
            created_at: <datetime> (from entry)
        ]
    ]
]

Two times of json stats:
1. game_sessions_summary - show total number of votes
2. game_sessions - show expanded structure of all sessions and votes
"""

import argparse
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, List
from uuid import UUID

import boto3
from spire.db import yield_db_read_only_session
from spire.journal.models import JournalEntry, JournalEntryTag
from sqlalchemy import func, text
from sqlalchemy.orm import Query, Session, aliased
from sqlalchemy.sql import text

from ..settings import (
    MOONSTREAM_S3_PUBLIC_DATA_BUCKET,
    MOONSTREAM_S3_PUBLIC_DATA_BUCKET_PREFIX,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")

yield_db_read_only_session_ctx = contextmanager(yield_db_read_only_session)


def push_stats(data: Any, filename: str) -> None:
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


def fetch_game_sessions_summary(db_session: Session, journal_id: UUID) -> List[Any]:
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
    game_sessions_summary = (
        db_session.query(
            entry_ids_game_session_ids.c.game_session_id.label("game_session_id"),
            func.count(entry_ids_game_session_ids.c.entry_id).label(
                "game_session_votes"
            ),
        )
        .group_by(entry_ids_game_session_ids.c.game_session_id)
        .order_by(text("game_session_votes DESC"))
    ).all()

    return game_sessions_summary


def fetch_game_sessions_obj_sql(db_session: Session, journal_id: UUID) -> List[Any]:
    """
    Fetch all votes and compose game session object.
    """
    game_session_tag = "game_session_id"
    stage_tag = "stage"
    path_tag = "path"
    client_id_tag = "client_id"

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
                    WHEN tag LIKE :client_id_tag || ':%' THEN substr(tag, POSITION(':' in tag) + 1)
                    ELSE NULL
                END as client_id,
                to_char(journal_entries.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created_at
        FROM journal_entry_tags JOIN journal_entries on journal_entry_tags.journal_entry_id = journal_entries.id
        WHERE journal_id = :journal_id
    ), grouping as (
        SELECT DISTINCT
            journal_entry_id,
            string_agg(game_session_id,',') as game_session_id,
            string_agg(stage,',') as stage,
            string_agg(path,',') as path,
            string_agg(client_id,',') as client_id,
            created_at
        FROM events_table
        GROUP BY 1,6
    ), paths as (SELECT
            game_session_id,
            stage,
            json_agg(json_build_object(
            :path_tag, path,
            :client_id_tag, client_id,
            'created_at', created_at
            )) as path_client_ids
        FROM grouping
        GROUP BY 1, 2
    ), stages as (SELECT
            game_session_id,
            json_agg(json_build_object(
            :stage_tag, stage, 
            'paths', path_client_ids
            )) as stage_path_client_ids
        FROM paths
        GROUP BY 1
    )SELECT
        json_agg(json_build_object(
        :game_session_tag,  game_session_id,
        'stages', stage_path_client_ids
        )) as json_data
        FROM stages;
    """
            ),
            {
                "game_session_tag": game_session_tag,
                "stage_tag": stage_tag,
                "path_tag": path_tag,
                "client_id_tag": client_id_tag,
                "journal_id": str(journal_id),
            },
        )
        .fetchone()
        .json_data
    )

    return result


def fetch_game_sessions_obj(db_session: Session, journal_id: UUID) -> List[Any]:
    """
    Fetch all votes and compose game session object.
    """
    # Game session IDs
    len_game_session_id_str = len("game_session_id:") + 1
    game_session_ids = (
        db_session.query(
            JournalEntryTag.journal_entry_id.label("entry_id"),
            func.substr(JournalEntryTag.tag, len_game_session_id_str).label(
                "game_session_id"
            ),
        )
        .select_from(JournalEntryTag)
        .filter(JournalEntryTag.tag.like("game_session_id:%"))
    ).subquery(name="game_session_ids")
    game_session_ids_alias = aliased(game_session_ids)

    # Stages
    len_stage_str = len("stage:") + 1
    stages = (
        db_session.query(
            JournalEntryTag.journal_entry_id.label("entry_id"),
            func.substr(JournalEntryTag.tag, len_stage_str).label("stage"),
        ).filter(JournalEntryTag.tag.like("stage:%"))
    ).subquery(name="stages")
    stages_alias = aliased(stages)

    # Paths
    len_path_str = len("path:") + 1
    paths = (
        db_session.query(
            JournalEntryTag.journal_entry_id.label("entry_id"),
            func.substr(JournalEntryTag.tag, len_path_str).label("path"),
        ).filter(JournalEntryTag.tag.like("path:%"))
    ).subquery(name="paths")
    paths_alias = aliased(paths)

    # Player IDs
    len_client_id_str = len("client_id:") + 1
    client_ids = (
        db_session.query(
            JournalEntryTag.journal_entry_id.label("entry_id"),
            func.substr(JournalEntryTag.tag, len_client_id_str).label("client_id"),
        ).filter(JournalEntryTag.tag.like("client_id:%"))
    ).subquery(name="client_id")
    client_ids_alias = aliased(client_ids)

    # Fetch votes
    votes_query = (
        db_session.query(
            JournalEntry.id.label("entry_id"),
            game_session_ids_alias.c.game_session_id.label("game_session_id"),
            stages_alias.c.stage.label("stage"),
            paths_alias.c.path.label("path"),
            client_ids_alias.c.client_id.label("client_id"),
            JournalEntry.created_at.label("created_at"),
        )
        .select_from(JournalEntry)
        .join(
            game_session_ids_alias, game_session_ids_alias.c.entry_id == JournalEntry.id
        )
        .join(stages_alias, stages_alias.c.entry_id == JournalEntry.id)
        .join(paths_alias, paths_alias.c.entry_id == JournalEntry.id)
        .join(client_ids_alias, client_ids_alias.c.entry_id == JournalEntry.id)
        .join(JournalEntryTag, JournalEntry.id == JournalEntryTag.journal_entry_id)
        .filter(JournalEntry.journal_id == journal_id)
        .group_by(
            JournalEntry.id,
            game_session_ids_alias.c.game_session_id,
            stages_alias.c.stage,
            paths_alias.c.path,
            client_ids_alias.c.client_id,
        )
        .order_by(text("created_at DESC"))
    ).subquery(name="votes_query")
    votes_query_alias = aliased(votes_query)

    # Build JSON obj with paths
    paths_query = (
        db_session.query(
            votes_query_alias.c.game_session_id,
            votes_query_alias.c.stage,
            func.array_agg(
                func.jsonb_build_object(
                    "path",
                    votes_query_alias.c.path,
                    "client_id",
                    votes_query_alias.c.client_id,
                    "created_at",
                    votes_query_alias.c.created_at,
                ),
            ).label("paths"),
        )
        .select_from(votes_query_alias)
        .group_by(votes_query_alias.c.game_session_id, votes_query_alias.c.stage)
    ).subquery(name="paths_query")
    paths_query_alias = aliased(paths_query)

    # Build JSON object with stages
    stages_query = (
        db_session.query(
            paths_query_alias.c.game_session_id,
            func.array_agg(
                func.jsonb_build_object(
                    "stage",
                    paths_query_alias.c.stage,
                    "paths",
                    paths_query_alias.c.paths,
                ),
            ).label("stages"),
        )
        .select_from(paths_query_alias)
        .group_by(paths_query_alias.c.game_session_id)
    ).subquery(name="stages_query")
    stages_query_alias = aliased(stages_query)

    # Build JSON object with game sessions
    game_sessions = db_session.query(
        func.array_agg(
            func.jsonb_build_object(
                "game_session_id",
                stages_query_alias.c.game_session_id,
                "stages",
                stages_query_alias.c.stages,
            ),
        ),
    ).select_from(stages_query_alias)

    game_sessions_obj = game_sessions.one_or_none()

    if game_sessions_obj is None:
        game_sessions_obj = []

    return game_sessions_obj


def stats_update_handler(args: argparse.Namespace) -> None:
    with yield_db_read_only_session_ctx() as db_session:
        game_sessions_summary_obj = fetch_game_sessions_summary(
            db_session=db_session, journal_id=args.journal
        )
        game_sessions_summary = []
        for obj in game_sessions_summary_obj:
            game_sessions_summary.append(list(obj))

        if args.push_to_bucket:
            push_stats(
                data=game_sessions_summary, filename="game_sessions_summary.json"
            )

        # # # SQLAlchemy implementation
        # # game_sessions_obj = fetch_game_sessions_obj(
        # #     db_session=db_session, journal_id=args.journal
        # # )
        # Raw SQL implementation
        game_sessions_obj = fetch_game_sessions_obj_sql(
            db_session=db_session, journal_id=args.journal
        )
        game_sessions = []
        for obj in game_sessions_obj:
            game_sessions.append(obj)

        if args.push_to_bucket:
            push_stats(data=game_sessions, filename="game_sessions.json")

        if not args.push_to_bucket:
            print(
                json.dumps(
                    {
                        "game_sessions_summary": game_sessions_summary,
                        "game_sessions": game_sessions,
                    }
                )
            )


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
        "--push-to-bucket",
        action="store_true",
        help="Push to AWS S3 bucket if argument set",
    )

    parser_stats_update.set_defaults(func=stats_update_handler)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
