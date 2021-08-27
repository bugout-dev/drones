import argparse
from datetime import datetime, timedelta
from distutils.util import strtobool
from typing import List, Optional
from uuid import UUID
import time

from spire.db import SessionLocal as session_local_spire
from brood.external import SessionLocal as session_local_brood
from brood.settings import BUGOUT_URL
from spire.journal.data import RuleActions
from spire.journal.models import JournalEntry, JournalEntryTag, JournalTTL

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


def journal_rules_execute_handler(args: argparse.Namespace) -> None:
    """
    Process ttl rules for journal entries.

    ttl - (drop or another action from rule) entries with timestamp less then current datetime minus 
    value from rule in seconds.
    tags - (drop or another action from rule) entries which contains tags in rule
    """
    db_session_spire = session_local_spire()
    try:
        rules_query = db_session_spire.query(JournalTTL).filter(
            JournalTTL.active == True
        )
        if args.id is not None:
            rules_query = rules_query.filter(JournalTTL.id == args.id)
        rules = rules_query.all()

        for rule in rules:
            print(f"Executing rule {str(rule.id)} for journal {str(rule.journal_id)}")
            entries_query = db_session_spire.query(JournalEntry).filter(
                JournalEntry.journal_id == rule.journal_id
            )

            for c_key, c_val in rule.conditions.items():
                if c_key == "ttl":
                    current_timestamp = datetime.now()
                    entries_query = entries_query.filter(
                        JournalEntry.updated_at
                        < (current_timestamp - timedelta(seconds=c_val))
                    )
                if c_key == "tags":
                    # For entries with tags in rule conditions
                    tags_query = db_session_spire.query(JournalEntryTag).filter(
                        JournalEntryTag.journal_entry_id.in_(
                            [entry.id for entry in entries_query]
                        ),
                        JournalEntryTag.tag.in_(c_val),
                    )
                    entries_query = entries_query.filter(
                        JournalEntry.id.in_(
                            [tag.journal_entry_id for tag in tags_query]
                        )
                    )

            if rule.action == RuleActions.remove.value:
                entries_to_drop_num = entries_query.delete(synchronize_session=False)
                db_session_spire.commit()
                print(
                    f"Dropped {entries_to_drop_num} for journal with id: {rule.journal_id}"
                )
    finally:
        db_session_spire.close()


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
        "-i", "--id", help="Humbug integration id",
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
        "migration", choices=MIGRATIONS, help="Which migration to run",
    )
    parser_migrate.add_argument(
        "action", choices=ACTIONS, help="Whether to run upgrade or downgrade",
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
    parser_rules_execute = subcommands_rules.add_parser(
        "execute", description="Execute ttl rule to journal"
    )

    parser_rules_execute.add_argument("-i", "--id", help="Rule ID")
    parser_rules_execute.set_defaults(func=journal_rules_execute_handler)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
