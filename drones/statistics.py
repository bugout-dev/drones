from datetime import date, datetime, timedelta
import json
import logging
import math
import os
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import boto3
from spire.journal import models as journals_models
from sqlalchemy.orm import Session
from sqlalchemy import func, text, and_
from spire.db import SessionLocal as session_local_spire

from .settings import DRONES_BUCKET, DRONES_BUCKET_STATISTICS_PREFIX


logger = logging.getLogger(__name__)

timescales_params: Dict[str, Dict[str, str]] = {
    "year": {"timestep": "1 day", "timeformat": "YYYY-MM-DD"},
    "month": {"timestep": "1 day", "timeformat": "YYYY-MM-DD"},
    "week": {"timestep": "1 day", "timeformat": "YYYY-MM-DD"},
    "day": {"timestep": "1 hours", "timeformat": "YYYY-MM-DD HH24"},
}

timescales_delta: Dict[str, Dict[str, timedelta]] = {
    "year": {"timedelta": timedelta(days=364)},
    "month": {"timedelta": timedelta(days=27)},
    "week": {"timedelta": timedelta(days=6)},
    "day": {"timedelta": timedelta(hours=24)},
}


def push_statistics(
    statistics_data: Dict[str, Any], journal_id: UUID, statistics_file: str
) -> None:
    if DRONES_BUCKET is None:
        logger.warning(
            "AWS_STATS_S3_BUCKET environment variable not defined, skipping storage of search results"
        )
        return

    result_bytes = json.dumps(statistics_data).encode("utf-8")
    result_key = f"{DRONES_BUCKET_STATISTICS_PREFIX}/{journal_id}/v5/{statistics_file}"

    s3 = boto3.client("s3")
    s3.put_object(
        Body=result_bytes,
        Bucket=DRONES_BUCKET,
        Key=result_key,
        ContentType="application/json",
        Metadata={"drone": "statistics"},
    )

    # TODO (Andrey) Understand why logger wont show messages some time and put them beside print
    # without print here exeption wont show.
    print(
        f"Statistics of {statistics_file.split('.')[0]} push to bucket: s3://{DRONES_BUCKET}/{result_key}"
    )


def get_journals_list(
    db_session: Session, journal_ids: Optional[List[UUID]] = None
) -> List[journals_models.Journal]:
    journals_query = db_session.query(journals_models.Journal)
    if journal_ids is not None:
        journals_query = journals_query.filter(
            journals_models.Journal.id.in_(journal_ids)
        )
    journals = journals_query.all()
    return journals


def get_journal(
    db_session: Session, journal_id: Optional[UUID] = None
) -> journals_models.Journal:
    journals_query = db_session.query(journals_models.Journal)
    if journal_id is not None:
        journals_query = journals_query.filter(journals_models.Journal.id == journal_id)
    else:
        raise
    journal = journals_query.one_or_none()
    return journal


def generate_tags_time_series(
    session: Any,
    journal_id: UUID,
    statistics_type: str,
    filter_condition: Any,
    time_step: str,
    time_mask: str,
    start: Optional[date] = None,
    end: Optional[datetime] = None,
):
    """
    Function generate time series in given range for given object

    """

    # create empty time series

    if end is None:
        end = datetime.utcnow()

    time_series_subquery = (
        session.query(
            func.generate_series(
                start,
                end,
                time_step,
            ).label("timeseries_points")
        )
    ).subquery(name="time_series_subquery")

    # get distinct tags labels in that range

    tags_requested = (
        session.query(journals_models.JournalEntryTag.tag.label("tag"))
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(filter_condition)
        .distinct()
    )

    if start is not None:
        tags_requested = tags_requested.filter(
            journals_models.JournalEntry.created_at > start
        )
    if end is not None:
        tags_requested = tags_requested.filter(
            journals_models.JournalEntry.created_at < end
        )

    tags_requested_subquery = tags_requested.subquery(name="tags_requested_subquery")

    # empty timeseries with tags
    empty_time_series_subquery = session.query(
        func.to_char(time_series_subquery.c.timeseries_points, time_mask).label(
            "timeseries_points"
        ),
        tags_requested_subquery.c.tag.label("tag"),
    ).subquery(name="empty_time_series_subquery")

    # tags count
    tags_counts = (
        session.query(
            func.to_char(journals_models.JournalEntryTag.created_at, time_mask).label(
                "timeseries_points"
            ),
            func.count(journals_models.JournalEntryTag.id).label("count"),
            journals_models.JournalEntryTag.tag.label("tag"),
        )
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(filter_condition)
    )

    if start is not None:
        tags_counts = tags_counts.filter(
            journals_models.JournalEntry.created_at > start
        )
    if end is not None:
        tags_counts = tags_counts.filter(journals_models.JournalEntry.created_at < end)

    tags_counts_subquery = (
        tags_counts.group_by(
            text("timeseries_points"),
            journals_models.JournalEntryTag.tag,
        )
        .order_by(text("timeseries_points desc"))
        .subquery(name="tags_counts_subquery")
    )

    # Join empty tags_time_series with tags count eg apply tags counts to time series.
    tags_time_series = (
        session.query(
            empty_time_series_subquery.c.timeseries_points.label("timeseries_points"),
            empty_time_series_subquery.c.tag,
            func.coalesce(tags_counts_subquery.c.count, 0),
        )
        .join(
            tags_counts_subquery,
            and_(
                empty_time_series_subquery.c.tag == tags_counts_subquery.c.tag,
                empty_time_series_subquery.c.timeseries_points
                == tags_counts_subquery.c.timeseries_points,
            ),
            isouter=True,
        )
        .order_by(text("timeseries_points DESC"))
    )

    response_tags: Dict[Any, Any] = {}

    for created_date, tag, count in tags_time_series:
        if not response_tags.get(tag):
            response_tags[tag] = []
            if time_mask == "YYYY-MM-DD HH24":
                created_date, hour = created_date.split(" ")
                response_tags[tag].append(
                    {"date": created_date, "hour": hour, "count": count}
                )
            else:
                response_tags[tag].append({"date": created_date, "count": count})
        else:
            if time_mask == "YYYY-MM-DD HH24":
                created_date, hour = created_date.split(" ")
                response_tags[tag].append(
                    {"date": created_date, "hour": hour, "count": count}
                )
            else:
                response_tags[tag].append({"date": created_date, "count": count})

    return response_tags


def generate_most_used_tags_summary(
    session: Any,
    journal_id: UUID,
    statistics_type: str,
    filter_condition: Any,
    start: Optional[date] = None,
    end: Optional[datetime] = None,
):
    """
    generate used tags summary
    """
    tags_summary = {}

    # Group by tags name and counting them
    tags_counts_query = (
        session.query(
            journals_models.JournalEntryTag.tag,
            func.count(journals_models.JournalEntryTag.id).label("count"),
        )
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(filter_condition)
    )

    if start is not None:
        tags_counts_query = tags_counts_query.filter(
            journals_models.JournalEntry.created_at > start
        )
    if end is not None:
        tags_counts_query = tags_counts_query.filter(
            journals_models.JournalEntry.created_at < end
        )

    tags_counts_query = (
        tags_counts_query.group_by(journals_models.JournalEntryTag.tag)
        .order_by(text("count DESC"))
        .limit(10)
    )

    for tag, count in tags_counts_query:
        tags_summary[tag] = count

    return tags_summary


def generate_events_summary(
    session: Any,
    journal_id: UUID,
    event_name: str,
    statistics_type: str,
    start: Optional[date] = None,
    end: Optional[datetime] = None,
):
    """
    generate session summary
    """
    events_summary: Dict[str, Any] = {}

    # Group by tags name and counting them

    entry_ids_with_tag = (
        session.query(
            journals_models.JournalEntryTag.journal_entry_id.label("entry_id"),
            journals_models.JournalEntryTag.tag.label("events"),
        )
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(journals_models.JournalEntryTag.tag.like(f"{event_name}:%"))
    )

    if start is not None:
        entry_ids_with_tag = entry_ids_with_tag.filter(
            journals_models.JournalEntry.created_at > start
        )
    if end is not None:
        entry_ids_with_tag = entry_ids_with_tag.filter(
            journals_models.JournalEntry.created_at < end
        )

    entry_ids_with_tag = entry_ids_with_tag.subquery(name="events")

    entry_ids_with_error_tag = (
        session.query(
            journals_models.JournalEntryTag.journal_entry_id.label("entry_id"),
            journals_models.JournalEntryTag.tag.label("errors"),
        )
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(journals_models.JournalEntryTag.tag.like("error:%"))
    )

    if start is not None:
        entry_ids_with_error_tag = entry_ids_with_error_tag.filter(
            journals_models.JournalEntry.created_at > start
        )
    if end is not None:
        entry_ids_with_error_tag = entry_ids_with_error_tag.filter(
            journals_models.JournalEntry.created_at < end
        )

    entry_ids_with_error_tag = entry_ids_with_error_tag.subquery(name="errors")

    # Group by tag

    events_counts = session.query(
        entry_ids_with_tag.c.events.label("event"),
        func.count(entry_ids_with_tag.c.entry_id).label("count"),
    ).group_by(entry_ids_with_tag.c.events)

    # Top 10 counts for that event
    top_10_group_by = events_counts.order_by(text("count desc")).limit(10)

    events_summary[f"common"] = {}
    for event, event_count in top_10_group_by:
        events_summary[f"common"][event] = event_count

    # Do as subquery
    events_counts = events_counts.subquery(name="events_counts")

    events_with_error_counts = (
        session.query(
            entry_ids_with_tag.c.events.label("event"),
            func.count(entry_ids_with_error_tag.c.errors).label("count"),
        )
        .join(
            entry_ids_with_error_tag,
            entry_ids_with_tag.c.entry_id == entry_ids_with_error_tag.c.entry_id,
        )
        .group_by(entry_ids_with_tag.c.events)
    ).subquery(name="events_with_error_counts")

    # Do group by count

    events_counts_distribution = (
        session.query(
            events_counts.c.count.label("group_count"),
            func.count(events_counts.c.event).label("events_count"),
        )
        .group_by(text("group_count"))
        .order_by(text("events_count DESC"))
    )

    events_with_errors_counts_distribution = (
        session.query(
            events_with_error_counts.c.count.label("group_count"),
            func.count(events_with_error_counts.c.event).label("events_count"),
        )
        .group_by(text("group_count"))
        .order_by(text("events_count DESC"))
    )

    total_event_count = 0
    events_summary[f"events_histogram"] = {}
    for group_count, event_count in events_counts_distribution:
        events_summary[f"events_histogram"][group_count] = event_count
        total_event_count += event_count

    total_event_errors_count = 0
    events_summary[f"errors_histogram"] = {}
    for group_count, event_count in events_with_errors_counts_distribution:
        events_summary[f"errors_histogram"][group_count] = event_count
        total_event_errors_count += event_count

    # 0 group count
    events_summary[f"errors_histogram"]["0"] = (
        total_event_count - total_event_errors_count
    )

    return events_summary


def entropy(count, total):
    """
    Calculate entropy
    """
    return (count / total) * math.log(total / count) + (
        (total - count) / total
    ) * math.log(total / (total - count))


def generate_highest_entropy_tags(
    session: Any,
    journal_id: UUID,
    statistics_type: str,
    filter_condition: Any,
    start: Optional[date] = None,
    end: Optional[datetime] = None,
):
    """
    Calculate entropy for each tag
    Tag with big entropy mean more usefull for big set of tags
    """

    if filter_condition == True:
        tag_filter = True
    else:
        tag_filter = (
            session.query(journals_models.JournalEntryTag)
            .filter(
                journals_models.JournalEntryTag.journal_entry_id
                == journals_models.JournalEntry.id
            )
            .filter(filter_condition)
            .exists()
        )

    # entries count in journal by given timerange

    entries_query = (
        session.query(journals_models.JournalEntry.id)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(tag_filter)
    )

    if start is not None:
        entries_query = entries_query.filter(
            journals_models.JournalEntry.created_at > start
        )

    if end is not None:
        entries_query = entries_query.filter(
            journals_models.JournalEntry.created_at < end
        )

    entries_total_count = entries_query.count()

    # tags counts
    tags_count = (
        session.query(
            func.count(journals_models.JournalEntryTag.id),
            journals_models.JournalEntryTag.tag.label("tag"),
        )
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(filter_condition)
    )

    if start is not None:
        tags_count = tags_count.filter(journals_models.JournalEntry.created_at > start)
    if end is not None:
        tags_count = tags_count.filter(journals_models.JournalEntry.created_at < end)

    tags_count = tags_count.group_by(journals_models.JournalEntryTag.tag)

    unsorted_tags = []

    for count, tag in tags_count:
        # For tag with count 0 and tag count == count_of_entries, entropy = 0
        if count == 0 or count / entries_total_count == 1:
            unsorted_tags.append((tag, 0))
            continue

        # calculate entropy for each tag
        unsorted_tags.append((tag, entropy(count, entries_total_count)))

    sorted_tags = sorted(unsorted_tags, reverse=True, key=lambda item: item[1])

    return {k: v for k, v in sorted_tags[:10]}


def entries_by_days(
    db_session: Session,
    journal_id: UUID,
    filter_condition: Any,
    start_day: Optional[date] = None,
):
    if filter_condition == True:
        tag_filter = True
    else:
        tag_filter = (
            db_session.query(journals_models.JournalEntryTag)
            .filter(
                journals_models.JournalEntryTag.journal_entry_id
                == journals_models.JournalEntry.id
            )
            .filter(filter_condition)
            .exists()
        )

    entries_values = (
        db_session.query(
            func.to_char(
                func.DATE(journals_models.JournalEntry.created_at), "YYYY-MM-DD"
            ).label("date"),
            func.count(journals_models.JournalEntry.id).label("cnt"),
        )
        .filter(tag_filter)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(func.DATE(journals_models.JournalEntry.created_at) > start_day)
        .group_by(func.DATE(journals_models.JournalEntry.created_at))
        .order_by(text("date desc"))
    )

    return [{"date": date, "count": count} for date, count in entries_values]


def get_statistics(
    db_session: Session,
    journal_id: UUID,
    statistics_type: str,
    timescale: str,
    time_step: str,
    time_mask: str,
    start_time: Optional[date] = None,
    end_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    current_timestamp = datetime.utcnow()

    year_ago = date.today() - timedelta(days=364)

    statistics_response: Dict[str, Any] = {
        "created_at": f"{current_timestamp}",
        "schema": f"https://github.com/bugout-dev/schemas/blob/main/drones/statistics/{statistics_type}_v5.schema.json",
        "data": {},
    }

    # Disribition of tags for session stats

    if statistics_type == "session" or statistics_type == "client":
        try:
            statistics_response["data"] = {}

            if timescale == "year":
                filter = journals_models.JournalEntryTag.tag.like(
                    f"{statistics_type}:%"
                )
                statistics_response["data"]["entries"] = entries_by_days(
                    db_session=db_session,
                    journal_id=journal_id,
                    start_day=start_time,
                    filter_condition=filter,
                )

            event_summary = generate_events_summary(
                session=db_session,
                journal_id=journal_id,
                event_name=statistics_type,
                statistics_type=statistics_type,
                start=start_time,
                end=end_time,
            )
            if statistics_type == "client":
                statistics_response["data"].update(
                    week_to_week_retention(
                        db_session=db_session,
                        journal_id=journal_id,
                        event_name=statistics_type,
                        statistics_type=statistics_type,
                        start=start_time,
                        end=end_time,
                    )
                )

            statistics_response["data"].update(event_summary)

        except Exception as err:
            logger.error(f"Generate session statistics end with error:{err}")

        return statistics_response

    try:
        if statistics_type == "errors":
            filter = journals_models.JournalEntryTag.tag.like("error:%")
        else:
            filter = True

        # Entries timeseries by days
        if timescale == "year":
            statistics_response["data"]["entries"] = entries_by_days(
                db_session=db_session,
                journal_id=journal_id,
                start_day=start_time,
                filter_condition=filter,
            )

        split_highest_entropy_tags = {}
        split_most_used_tags = {}

        # Generate top 10
        # common tags / tags entropy for errros and stats

        # Entropy
        split_highest_entropy_tags = generate_highest_entropy_tags(
            session=db_session,
            journal_id=journal_id,
            statistics_type=statistics_type,
            filter_condition=filter,
            start=start_time,
            end=end_time,
        )

        # Common tags
        split_most_used_tags = generate_most_used_tags_summary(
            session=db_session,
            journal_id=journal_id,
            statistics_type=statistics_type,
            start=start_time,
            end=end_time,
            filter_condition=filter,
        )

        # Calculate timeseries

        if statistics_type == "stats":
            most_common_tags_labels = list(split_most_used_tags)
            most_highest_tags_labels = list(split_highest_entropy_tags)

            # Create filter for generate timesseries for that tags
            filter = journals_models.JournalEntryTag.tag.in_(
                set(most_common_tags_labels + most_highest_tags_labels)
            )

        timeseries = generate_tags_time_series(
            session=db_session,
            journal_id=journal_id,
            start=start_time,
            end=end_time,
            statistics_type=statistics_type,
            filter_condition=filter,
            time_step=time_step,
            time_mask=time_mask,
        )

        if statistics_type == "errors":
            statistics_response["data"]["most_common_errors"] = split_most_used_tags
            statistics_response["data"][
                "highest_entropy_tags"
            ] = split_highest_entropy_tags
            statistics_response["data"]["errors_time_series"] = timeseries

        else:
            statistics_response["data"]["most_common_tags"] = split_most_used_tags
            statistics_response["data"][
                "highest_entropy_tags"
            ] = split_highest_entropy_tags
            statistics_response["data"]["tags"] = timeseries

    except Exception as err:
        logger.error(f"Generate statistics end with error:{err}")

    return statistics_response


def week_to_week_retention(
    db_session: Session,
    journal_id: UUID,
    event_name: str,
    statistics_type: str,
    start: Optional[date] = None,
    end: Optional[datetime] = None,
) -> Dict[str, List[Dict[str, Union[str, int]]]]:
    """
    Generate week to week retention stats for given journal

    return list of weeks with retention and wau(weekly active users)

    """

    clients_events = (
        db_session.query(
            journals_models.JournalEntryTag.tag.label("client"),
            func.date_trunc("week", journals_models.JournalEntryTag.created_at).label(
                "created_at_week"
            ),
        )
        .distinct()
        .join(journals_models.JournalEntry)
        .filter(journals_models.JournalEntry.journal_id == journal_id)
        .filter(journals_models.JournalEntryTag.tag.like("client:%"))
    )
    if start is not None:
        clients_events = clients_events.filter(
            journals_models.JournalEntryTag.created_at > start
        )
    if end is not None:
        clients_events = clients_events.filter(
            journals_models.JournalEntryTag.created_at < end
        )

    clients_events_with = clients_events.cte(name="clients_events")

    week1 = clients_events_with.alias("week1")
    week2 = clients_events_with.alias("week2")

    transitions = db_session.query(
        week1.c.client.label("client"),
        week2.c.client.label("client_redundant"),
        week1.c.created_at_week.label("week1"),
        week2.c.created_at_week.label("week2"),
    ).join(
        week2,
        and_(
            week2.c.created_at_week == (week1.c.created_at_week + timedelta(weeks=1)),
            week2.c.client == week1.c.client,
        ),
    )

    transitions_with = transitions.cte("transitions")

    wau = db_session.query(
        clients_events_with.c.created_at_week.label("created_at_week"),
        func.count(func.distinct(clients_events_with.c.client)).label("wau"),
    ).group_by(text("created_at_week"))

    wau_with = wau.cte("wau")

    week_to_week_retention_query = (
        db_session.query(
            wau_with.c.created_at_week.label("week"),
            wau_with.c.wau.label("wau"),
            func.count(func.distinct(transitions_with.c.client)).label(
                "num_returning_clients"
            ),
        )
        .join(
            transitions_with,
            transitions_with.c.week2 == wau_with.c.created_at_week,
            isouter=True,
        )
        .group_by(wau_with.c.created_at_week, wau_with.c.wau)
    )

    week_to_week_retention: Dict[str, List[Dict[str, Union[str, int]]]] = {
        "retention": []
    }
    for index, (week, wau, num_returning_clients) in enumerate(
        week_to_week_retention_query
    ):
        week_to_week_retention["retention"].append(
            {
                "week": str(week),
                "week_index": index,
                "wau": wau,
                "num_returning_clients": num_returning_clients,
            }
        )

    return week_to_week_retention


def generate_and_push(
    db_session: Session,
    journal_id: UUID,
    timescale: str,
    stats_type: str,
    push_to_bucket: bool = True,
):
    """
    Create stats and push to S3
    """

    db_session_spire = session_local_spire()

    try:
        start_date = datetime.utcnow() - timescales_delta[timescale]["timedelta"]

        time_step = timescales_params[timescale]["timestep"]

        time_mask = timescales_params[timescale]["timeformat"]

        statistics_data = get_statistics(
            db_session=db_session_spire,
            journal_id=journal_id,
            statistics_type=stats_type,
            timescale=timescale,
            start_time=start_date,
            time_step=time_step,
            time_mask=time_mask,
        )

        if push_to_bucket:
            push_statistics(
                statistics_data=statistics_data,
                journal_id=journal_id,
                statistics_file=f"{timescale}/{stats_type}.json",
            )
    except Exception as err:
        print(
            f"Unexpected error occurred due generating statistics by drone: {str(err)}"
        )
    finally:
        db_session_spire.close()


def generate_and_push_in_own_process(
    journal_id: UUID,
    stats_types: List[str],
    timescales: List[str],
    push_to_bucket: bool = True,
) -> None:
    db_session_spire = session_local_spire()
    try:
        journal = get_journal(db_session=db_session_spire, journal_id=journal_id)
        if not journal:
            print("Journal not found.")
            raise
        for timescale in timescales:
            for stats_type in stats_types:
                generate_and_push(
                    db_session=db_session_spire,
                    journal_id=journal_id,
                    stats_type=stats_type,
                    timescale=timescale,
                    push_to_bucket=push_to_bucket,
                )
    except Exception as err:
        print(
            f"Unexpected error occurred due generating statistics by drone: {str(err)}"
        )
    finally:
        db_session_spire.close()
