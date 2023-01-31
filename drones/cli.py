import argparse
from datetime import datetime, timedelta
from distutils.util import strtobool
import logging
from typing import List, Optional
from uuid import UUID
import time

from brood.external import SessionLocal as session_local_brood
from brood.settings import BUGOUT_URL
from spire.db import (
    SessionLocal as session_local_spire,
    create_spire_engine,
    SPIRE_DB_URI,
    BUGOUT_SPIRE_THREAD_DB_POOL_SIZE,
    BUGOUT_SPIRE_THREAD_DB_MAX_OVERFLOW,
    SPIRE_DB_POOL_RECYCLE_SECONDS,
)
from spire.journal.models import JournalEntryLock, JournalEntry, Journal
from sqlalchemy import func, text
from sqlalchemy.orm import sessionmaker

from .data import (
    StatsTypes,
    TimeScales,
    RedisPickCommand,
)
from . import reports
from . import statistics
from .migrations import ACTIONS, MIGRATIONS
from .humbug_reports import process_humbug_tasks_queue, pick_humbug_tasks_queue
from .settings import (
    REPORTS_CHUNK_SIZE,
    WAITING_UNTIL_NEW_REPORTS,
    REDIS_REPORTS_QUEUE,
    REDIS_FAILED_REPORTS_QUEUE,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def reports_generate_handler(args: argparse.Namespace):
    """
    Generates report for Humbug integration.
    """
    db_session_spire = session_local_spire()
    db_session_brood = session_local_brood()
    try:
        humbug_integrations = reports.get_humbug_integrations(
            db_session=db_session_spire, event_id=args.id
        )

        for humbug_integration in humbug_integrations:
            group_data = reports.obtain_group_integration_data(
                db_session_spire=db_session_spire,
                db_session_brood=db_session_brood,
                humbug_integration=humbug_integration,
            )
            if group_data is None:
                print(
                    f"No entries to work with in integration: {humbug_integration.id}"
                )
                continue
            elif group_data.available_events < group_data.num_events_month:
                print(
                    f"\n Humbug integration id {humbug_integration.id}. "
                    f"Exceeded the limit by {group_data.num_events_month - group_data.available_events} events "
                )
                if bool(strtobool(args.send)) is True:
                    content = reports.generate_response(
                        group_name=group_data.group_name,
                        num_events_total=group_data.num_events_total,
                        num_events_month=group_data.num_events_month,
                        available_events=group_data.available_events,
                    )
                    billing_url = f"{BUGOUT_URL}/account/teams?team_id={group_data.group_id}&manage_subscription=true"
                    content += (
                        f"You have exceeded the limit by <b>{group_data.num_events_month - group_data.available_events}</b> more. "
                        f"To be able to continue receiving events please update your subscriptions with following link:<br>"
                        f"{billing_url}<br>"
                    )
                    for owner in group_data.group_owners:
                        reports.send_billing_report_email(
                            content=content, email=owner.email
                        )
                    print("Billing reports sent.")
            elif (
                (group_data.available_events / 2)
                >= group_data.num_events_month
                > group_data.available_free_events
            ):
                print(
                    f"\n Humbug integration id {humbug_integration.id}. "
                    f"Subscription can be reduced, available events: {group_data.available_events}, "
                    f"events this month: {group_data.num_events_month}"
                )
                if bool(strtobool(args.send)) is True:
                    content = reports.generate_response(
                        group_name=group_data.group_name,
                        num_events_total=group_data.num_events_total,
                        num_events_month=group_data.num_events_month,
                        available_events=group_data.available_events,
                    )
                    billing_url = f"{BUGOUT_URL}/account/teams?team_id={group_data.group_id}&manage_subscription=true"
                    content += (
                        f"You can reduce your subscription with following link:<br>"
                        f"{billing_url}<br>"
                    )
                    for owner in group_data.group_owners:
                        reports.send_billing_report_email(
                            content=content, email=owner.email
                        )
                    print("Billing reports sent.")
    except Exception as err:
        print(f"Unexpected error occurred due work with reports: {str(err)}")
    finally:
        db_session_spire.close()
        db_session_brood.close()


def statistics_generate_handler(args: argparse.Namespace):
    db_session_spire = session_local_spire()

    try:
        journal_ids: Optional[List[UUID]] = None
        if args.journals:
            journal_ids = [UUID(journal_id_raw) for journal_id_raw in args.journals]
        journals = statistics.get_journals_list(db_session_spire, journal_ids)

        available_types = [stats_type.value for stats_type in StatsTypes]
        available_timescales = [stats_type.value for stats_type in TimeScales]

        if args.types:
            stats_types = [
                stats_type for stats_type in args.types if stats_type in available_types
            ]
        else:
            stats_types = available_types

        for journal in journals:

            current_timestamp = datetime.utcnow()

            timesscales = available_timescales

            # Usual statistics generate

            print(f"Generate statistics for journal: {journal.id} ")
            start_time = time.time()
            for timescale in timesscales:

                print(f"Time scale: {timescale}")

                for statistics_type in ["stats", "errors", "session", "client"]:

                    if journal.search_index is not None and statistics_type in [
                        "errors",
                        "session",
                        "client",
                    ]:
                        continue

                    statistics.generate_and_push(
                        db_session=db_session_spire,
                        journal_id=journal.id,
                        timescale=timescale,
                        stats_type=statistics_type,
                    )
            print(
                f"Generate stats. Generation time: {(time.time() - start_time):.3f} seconds"
            )

    except Exception as err:
        print(
            f"Unexpected error occurred due generating statistics by drone: {str(err)}"
        )
    finally:
        db_session_spire.close()


def push_reports_from_redis(args: argparse.Namespace):

    db_session_spire = session_local_spire()

    try:
        process_humbug_tasks_queue(
            db_session=db_session_spire,
            queue_key=args.queue_key,
            upload_command=args.command,
            chunk_size=args.chunk_size,
            block=args.blocking,
            timeout=args.timeout,
        )
    except Exception as err:
        print(
            f"Unexpected error occurred due processing humbug reports statistics by drone: {str(err)}"
        )
    finally:
        db_session_spire.close()


def pick_reports_from_redis(args: argparse.Namespace):
    try:
        pick_humbug_tasks_queue(
            queue_key=args.queue_key,
            command=args.command,
            chunk_size=args.chunk_size,
            start=args.start_index,
        )
    except Exception as err:
        print(f"Unexpected error on pick command: {str(err)}")


def migration_handler(args: argparse.Namespace):
    action = MIGRATIONS[args.migration][args.action]
    action(args.journal, args.debug)


def journal_rules_unlock_handler(args: argparse.Namespace) -> None:
    """
    Removes locks from entries.
    """
    current_timestamp = datetime.now()
    db_session_spire = session_local_spire()
    try:
        query = db_session_spire.query(JournalEntryLock).filter(
            JournalEntryLock.locked_at
            < (current_timestamp - timedelta(seconds=args.ttl))
        )
        objects_to_drop_num = query.delete(synchronize_session=False)
        db_session_spire.commit()
        logger.info(f"Dropped {objects_to_drop_num} locks")
    except Exception as err:
        logger.error(f"Unable to drop locks, error: {str(err)}")
        db_session_spire.rollback()
    finally:
        db_session_spire.close()


def journal_entries_cleanup_handler(args: argparse.Namespace) -> None:

    """
    Clean entries from journal.
    """

    Green = "\033[0;32m"
    Yellow = "\033[0;33m"
    Red = "\033[0;31m"
    NC = "\033[0m"

    custom_engine = create_spire_engine(
        url=SPIRE_DB_URI,
        pool_size=BUGOUT_SPIRE_THREAD_DB_POOL_SIZE,
        max_overflow=BUGOUT_SPIRE_THREAD_DB_MAX_OVERFLOW,
        pool_recycle=SPIRE_DB_POOL_RECYCLE_SECONDS,
        statement_timeout=300000,  # SPIRE_DB_STATEMENT_TIMEOUT_MILLIS
    )

    process_session = sessionmaker(bind=custom_engine)
    db_session = process_session()

    chunk_size = args.batch_size
    try:
        # get counts per journal

        entries_count_per_journal = (
            db_session.query(
                func.count(JournalEntry.id).label("entries_count"),
                Journal.name,
                Journal.id,
            )
            .join(Journal, Journal.id == JournalEntry.journal_id)
            .filter(Journal.search_index == None)
            .group_by(Journal.name, Journal.id)
        )

        for entries_count, name, journal_id in entries_count_per_journal:

            if entries_count > args.max_entries:

                print(
                    f"Journal: {Yellow}{name}{NC} ({Green}{journal_id}{NC}) has {Yellow}{entries_count}{NC} entries. Delete {Red}{entries_count - args.max_entries}{NC} entries"
                )

                row_numbers = (
                    db_session.query(
                        JournalEntry.id,
                        func.row_number()
                        .over(order_by=text("created_at desc"))
                        .label("entry_number"),
                    )
                    .filter(JournalEntry.journal_id == journal_id)
                    .cte("row_numbers")
                )

                # delete entries in chunks

                deleting_range = list(
                    range(args.max_entries, entries_count, chunk_size)
                )

                deleting_range.reverse()  # start from end

                for entry_count in deleting_range:
                    print(f"Delete entries from entry_number > {entry_count}")

                    start = time.time()

                    delete_statment = (
                        db_session.query(JournalEntry)
                        .filter(
                            row_numbers.c.id == JournalEntry.id,
                            row_numbers.c.entry_number > entry_count,
                        )
                        .delete(synchronize_session=False)
                    )
                    print(f"Amount of deleted entries: {delete_statment}")
                    print(f"Time: {time.time() - start}")
                    db_session.commit()

    except Exception as err:
        logger.error(f"Unable to clean journal entries, error: {str(err)}")
        db_session.rollback()


def cleanup_reports_handler(args: argparse.Namespace) -> None:

    """
    Returns entries count for journals.
    """

    Green = "\033[0;32m"
    Yellow = "\033[0;33m"
    Red = "\033[0;31m"
    NC = "\033[0m"

    db_session_spire = session_local_spire()
    try:
        # get counts per journal

        query = (
            db_session_spire.query(
                func.count(JournalEntry.id).label("entries_count"),
                Journal.name,
                Journal.id,
            )
            .join(Journal, Journal.id == JournalEntry.journal_id)
            .filter(Journal.search_index == None)
            .group_by(Journal.name, Journal.id)
        )
        for entries_count, name, journal_id in query:
            print(f"Journal {journal_id} has {entries_count} entries")
            print(
                f"Cleanup journal {Green}{journal_id}{NC}:{Yellow}{name}{NC} will remove {Red}{entries_count - args.max_entries if entries_count - args.max_entries > 0 else 0 }{NC} entries"
            )

    except Exception as err:
        logger.error(f"Unable to get journal entries count, error: {str(err)}")
        db_session_spire.rollback()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Command Line Interface for Bugout drones"
    )
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(description="Drones commands")

    # Reports parser
    parser_reports = subcommands.add_parser("reports", description="Drone reports")
    parser_reports.set_defaults(func=lambda _: parser_reports.print_help())
    subcommands_reports = parser_reports.add_subparsers(
        description="Drone reports commands"
    )
    parser_reports_generate = subcommands_reports.add_parser(
        "generate", description="Generate reports for Humbug integration"
    )
    parser_reports_generate.add_argument(
        "-i",
        "--id",
        help="Humbug integration id",
    )
    parser_reports_generate.add_argument(
        "-s",
        "--send",
        required=True,
        choices=["True", "False"],
        help="Send or not report to owners",
    )
    parser_reports_generate.set_defaults(func=reports_generate_handler)

    # Statistics parser
    parser_statistics = subcommands.add_parser(
        "statistics", description="Drone statistics"
    )
    parser_statistics.set_defaults(func=lambda _: parser_statistics.print_help())
    subcommands_statistics = parser_statistics.add_subparsers(
        description="Drone statistics commands"
    )
    parser_statistics_generate = subcommands_statistics.add_parser(
        "generate", description="Generate statistics for Humbug integration"
    )
    parser_statistics_generate.add_argument(
        "journals", nargs="*", help="Journal IDs for which to generate statistics"
    )

    parser_statistics_generate.add_argument(
        "-t",
        "--types",
        type=str,
        choices=[stats_type.value for stats_type in StatsTypes],
        required=False,
        default=None,
        help="(Optional) Stats type wich will generate.",
    )

    parser_statistics_generate.set_defaults(func=statistics_generate_handler)

    parser_migrate = subcommands.add_parser(
        "migrate", description="Upgrade across database"
    )
    parser_migrate.add_argument(
        "migration",
        choices=MIGRATIONS,
        help="Which migration to run",
    )
    parser_migrate.add_argument(
        "action",
        choices=ACTIONS,
        help="Whether to run upgrade or downgrade",
    )
    parser_migrate.add_argument(
        "-j",
        "--journal",
        type=UUID,
        required=False,
        default=None,
        help="(Optional) Journal ID on which to run data migration",
    )

    parser_migrate.add_argument(
        "--debug", action="store_true", help="Set this flag to run in debug mode"
    )
    parser_migrate.set_defaults(func=migration_handler)

    parser_humbug_reports = subcommands.add_parser(
        "humbug_reports", description="Drone humbug reports"
    )
    parser_humbug_reports.set_defaults(
        func=lambda _: parser_humbug_reports.print_help()
    )
    subcommands_humbug_reports = parser_humbug_reports.add_subparsers(
        description="Drone humbug reports commands"
    )
    polling_command = subcommands_humbug_reports.add_parser(
        "start_polling", description="Pushed cached humbug reports to database"
    )
    polling_command.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=WAITING_UNTIL_NEW_REPORTS,
        help="Timeout for blocking mode, before trying to fetch new reports.",
    )
    polling_command.add_argument(
        "-b",
        "--blocking",
        action="store_true",
        help="true: Mode of processing reports wait for a new entry. false: Processed until the end of reports.",
    )
    polling_command.add_argument(
        "-n",
        "--chunk-size",
        type=int,
        default=REPORTS_CHUNK_SIZE,
        help="Size of processing reports at the moment.",
    )
    polling_command.add_argument(
        "-q",
        "--queue-key",
        type=str,
        default=REDIS_REPORTS_QUEUE,
        help="Queue from which you want geting data.",
    )
    polling_command.add_argument(
        "-c",
        "--command",
        type=str,
        choices=[commands.value for commands in RedisPickCommand],
        default="lpop",
        help="Redis command for extracting data from the queue.",
    )
    polling_command.set_defaults(func=push_reports_from_redis)

    pick_command = subcommands_humbug_reports.add_parser(
        "peek", description="Pushed cached humbug reports to database"
    )
    pick_command.add_argument(
        "-n",
        "--chunk-size",
        type=int,
        default=REPORTS_CHUNK_SIZE,
        help="Size of processing reports at the moment.",
    )
    pick_command.add_argument(
        "-q",
        "--queue-key",
        type=str,
        default=REDIS_FAILED_REPORTS_QUEUE,
        help=f"Queue from which you want to get data current reports queue {{{REDIS_REPORTS_QUEUE}}} current errors queue {{{REDIS_FAILED_REPORTS_QUEUE}}}.",
    )
    pick_command.add_argument(
        "-s",
        "--start-index",
        type=int,
        default=0,
        help="Queue from which you want to get data.",
    )
    pick_command.add_argument(
        "-c",
        "--command",
        type=str,
        choices=[commands.value for commands in RedisPickCommand],
        default="lrange",
        help=f"Redis command for extracting data from queue.",
    )
    pick_command.set_defaults(func=pick_reports_from_redis)

    # Journal ttl rules parser
    parser_rules = subcommands.add_parser(
        "rules", description="Drone journal ttl rules"
    )
    parser_rules.set_defaults(func=lambda _: parser_rules.print_help())
    subcommands_rules = parser_rules.add_subparsers(
        description="Drone journal ttl rules commands"
    )
    parser_rules_unlock = subcommands_rules.add_parser(
        "unlock", description="Drops locks of entries"
    )
    parser_rules_unlock.add_argument(
        "-t",
        "--ttl",
        type=int,
        default=300,
        help="Drop locks not earlier than the specified value in seconds",
    )
    parser_rules_unlock.set_defaults(func=journal_rules_unlock_handler)

    # Clean up parser

    parser_cleanup = subcommands.add_parser("cleanup", description="Drone cleanup")
    parser_cleanup.set_defaults(func=lambda _: parser_cleanup.print_help())

    subcommands_cleanup = parser_cleanup.add_subparsers(
        description="Drone cleanup commands"
    )

    parser_cleanup_journals = subcommands_cleanup.add_parser(
        "journals", description="Clean up journals"
    )
    parser_cleanup_journals.add_argument(
        "--max-entries",
        type=int,
        default=1000000,
        help="Max number of entries in journal",
    )
    parser_cleanup_journals.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Number of entries to delete in one batch",
    )

    parser_cleanup_journals.set_defaults(func=journal_entries_cleanup_handler)

    parser_cleanup_reports = subcommands_cleanup.add_parser(
        "reports", description="Clean up reports"
    )
    parser_cleanup_reports.add_argument(
        "--max-entries",
        type=int,
        default=1000000,
        help="Max number of entries in journal",
    )
    parser_cleanup_reports.set_defaults(func=cleanup_reports_handler)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
