import argparse
from datetime import datetime
from distutils.util import strtobool
from typing import List, Optional
from uuid import UUID
import time

from spire.db import SessionLocal as session_local_spire
from brood.external import SessionLocal as session_local_brood
from brood.settings import BUGOUT_URL

from .data import StatsTypes, TimeScales
from . import reports
from . import statistics
from .migrations import ACTIONS, MIGRATIONS


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


def migration_handler(args: argparse.Namespace):
    action = MIGRATIONS[args.migration][args.action]
    action(args.journal, args.debug)


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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
