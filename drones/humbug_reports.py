import json
from uuid import uuid4

from redis.client import Pipeline

from .data import HumbugCreateReportTask
import redis
from spire.journal import models as journal_models
from spire.humbug import models as humbug_models
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import aliased
from .db import yield_redis_connection_from_env_ctx


from .settings import REPORTS_CHUNK_SIZE


def process_humbug_tasks_queue(db_session: Session):

    with yield_redis_connection_from_env_ctx() as redis_client:

        count = redis_client.execute_command("LLEN", "reports_queue") // 100

        for _ in range(count):
            # logic of get all new reports
            reports_json = redis_client.execute_command(
                "LPOP", "reports_queue", REPORTS_CHUNK_SIZE
            )

            # remove duplicates
            deduplicate_records = list(set(reports_json))

            # parse tasks objects
            humbug_report_tasks = [
                HumbugCreateReportTask(**json.loads(report))
                for report in deduplicate_records
            ]

            # fetching pairs of journal ids and tokens

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
                        tuple(
                            set(
                                [str(task.bugout_token) for task in humbug_report_tasks]
                            )
                        )
                    )
                )
                .filter(
                    db_session.query(journal_models.Journal)
                    .filter(journal_models.Journal.id == humbug_event_alias.journal_id)
                    .exists()
                )
                .distinct()
            )

            journal_by_token = dict(journal_and_tokens.all())

            reports_pack = []
            reports_tags_pack = []

            for report_task in humbug_report_tasks:

                if not journal_by_token.get(report_task.bugout_token):
                    continue

                entry_id = uuid4()
                reports_pack.append(
                    journal_models.JournalEntry(
                        id=entry_id,
                        journal_id=str(journal_by_token[report_task.bugout_token]),
                        title=report_task.report.title,
                        content=report_task.report.content,
                        context_id=str(report_task.bugout_token),
                        context_type="humbug",
                    )
                )
                if report_task.report.tags is not None:
                    reports_tags_pack += [
                        journal_models.JournalEntryTag(
                            journal_entry_id=entry_id, tag=tag
                        )
                        for tag in report_task.report.tags
                        if tag
                    ]

            db_session.bulk_save_objects(reports_pack)
            db_session.commit()

            db_session.bulk_save_objects(reports_tags_pack)
            db_session.commit()
            print(f"{len(humbug_report_tasks)} pushed to database.")
