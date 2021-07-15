import json
from os import error
import time

from redis import Redis
from typing import Dict, Optional, List

from .data import HumbugCreateReportTask, HumbugFiledReportTask
import redis
from spire.journal import models as journal_models
from spire.humbug import models as humbug_models
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import aliased
from .db import yield_redis_connection_from_env_ctx


from .settings import (
    REPORTS_CHUNK_SIZE,
    WAITING_UNTIL_NEW_REPORTS,
    REDIS_REPORTS_QUEUE,
    REDIS_FILED_REPORTS,
)


def upload_report_tasks(
    redis_client: Redis, queue_key: str, command: str, chunk_size: int
):

    """
    Return parsed reports from redis
    
    """
    if command == "lrange":
        reports_json = redis_client.execute_command(
            command, queue_key, 0, chunk_size - 1
        )
    else:
        reports_json = redis_client.execute_command(command, queue_key, chunk_size)
    try:
        if reports_json is not None:
            # parse reports object
            return [
                HumbugCreateReportTask(**json.loads(report)) for report in reports_json
            ]
    except Exception as err:
        redis_client.rpush(
            REDIS_FILED_REPORTS, *reports_json,
        )


def get_journal_ids_by_tokens(
    db_session: Session, report_tasks: List[HumbugCreateReportTask]
):
    """
    Generate journal_id by tokens key value pairs
    """

    humbug_event_alias = aliased(humbug_models.HumbugEvent)

    journal_and_tokens = (
        db_session.query(
            humbug_models.HumbugBugoutUserToken.restricted_token_id,
            humbug_models.HumbugEvent.journal_id,
        )
        .join(
            humbug_models.HumbugBugoutUserToken,
            humbug_models.HumbugEvent.id
            == humbug_models.HumbugBugoutUserToken.event_id,
        )
        .filter(
            humbug_models.HumbugBugoutUserToken.restricted_token_id.in_(
                tuple(set([str(task.bugout_token) for task in report_tasks]))
            )
        )
        .filter(
            db_session.query(journal_models.Journal)
            .filter(journal_models.Journal.id == humbug_event_alias.journal_id)
            .exists()
        )
        .distinct()
    )

    return {str(token): journal_id for token, journal_id in journal_and_tokens.all()}


def push_to_database(
    db_session: Session,
    redis_client: Redis,
    report_tasks: List[HumbugCreateReportTask],
    journal_by_token: Dict[str, str],
):

    """
    Push all reports to database in one chunk
    """

    for report_task in report_tasks:
        try:

            if not journal_by_token.get(str(report_task.bugout_token)):
                continue

            entry_object = journal_models.JournalEntry(
                journal_id=journal_by_token[str(report_task.bugout_token)],
                title=report_task.report.title,
                content=report_task.report.content,
                context_id=str(report_task.bugout_token),
                context_type="humbug",
            )

            report_task.report.tags.append(
                f"reporter_token:{str(report_task.bugout_token)}"
            )

            entry_object.tags.extend(
                [
                    journal_models.JournalEntryTag(tag=tag)
                    for tag in report_task.report.tags
                    if tag
                ]
            )

            db_session.add(entry_object)
            db_session.commit()
        except Exception as err:
            redis_client.rpush(
                REDIS_FILED_REPORTS,
                HumbugFiledReportTask(
                    bugout_token=report_task.bugout_token,
                    report=report_task.report,
                    error=str(err),
                ).json(),
            )


def process_humbug_tasks_queue(
    db_session: Session,
    queue_key: str,
    upload_command: str,
    chunk_size: int,
    block: bool,
    timeout: int,
):
    with yield_redis_connection_from_env_ctx() as redis_client:
        print("Polling reports queue start")
        while True:
            # logic of get all new reports
            try:

                report_tasks = upload_report_tasks(
                    redis_client=redis_client,
                    queue_key=queue_key,
                    command=upload_command,
                    chunk_size=chunk_size,
                )

                if not report_tasks:
                    if block:
                        time.sleep(timeout)
                        continue
                    else:
                        return

                # fetching pairs of journal ids and tokens
                journal_by_token = get_journal_ids_by_tokens(
                    db_session=db_session, report_tasks=report_tasks
                )

                push_to_database(
                    db_session=db_session,
                    redis_client=redis_client,
                    report_tasks=report_tasks,
                    journal_by_token=journal_by_token,
                )
                print(f"{len(report_tasks)} pushed to database")

            except Exception as err:
                print(err)


def pick_humbug_tasks_queue(
    queue_key: str, command: str, chunk_size: int, start: int,
):

    with yield_redis_connection_from_env_ctx() as redis_client:
        if command == "lrange":
            reports_json = redis_client.execute_command(
                command, queue_key, start, chunk_size - 1
            )
        else:
            reports_json = redis_client.execute_command(command, queue_key, chunk_size)
        if reports_json:
            for i in reports_json:
                print(reports_json)
