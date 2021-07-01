from datetime import datetime, date
import logging
from typing import Optional
from uuid import UUID

from brood.data import SubscriptionPlanType
from brood.models import (
    Role,
    User,
    Group,
    GroupUser,
    Subscription,
    SubscriptionPlan,
    KVBrood,
)
from brood.settings import BUGOUT_FROM_EMAIL, SENDGRID_API_KEY
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from spire.journal.models import Journal, JournalEntry
from spire.humbug.models import HumbugEvent
from sqlalchemy import func
from sqlalchemy.orm import Session

from .data import GroupOwner, GroupIntegrationData

logger = logging.getLogger(__name__)


def generate_response(
    group_name: str,
    num_events_total: int,
    num_events_month: int,
    available_events: int,
) -> str:
    content = (
        f"<h3 align='center'>Crash Reports statistics for \"{group_name}\" team</h3><br>"
        f"Number of total received events: {num_events_total}.<br>"
        f"Number of events received this month: {num_events_month}.<br>"
        f"Available number of receiving events per month according with your subscription: {available_events}.<br>"
        f"<br>"
    )
    return content


def get_humbug_integrations(
    db_session: Session, event_id: Optional[UUID] = None
) -> HumbugEvent:
    query = db_session.query(HumbugEvent)
    if event_id is not None:
        query = query.filter(HumbugEvent.id == event_id)
    events = query.all()
    return events


def obtain_group_integration_data(
    db_session_spire: Session,
    db_session_brood: Session,
    humbug_integration: HumbugEvent,
) -> Optional[GroupIntegrationData]:
    current_timestamp = datetime.now()
    start_of_month_timestamp = date(current_timestamp.year, current_timestamp.month, 1)

    journal_data = (
        db_session_spire.query(Journal.name, func.count(JournalEntry.id))
        .join(JournalEntry, JournalEntry.journal_id == Journal.id)
        .filter(Journal.id == humbug_integration.journal_id)
        .group_by(Journal.id)
        .one_or_none()
    )
    if journal_data is None:
        return None

    journal_data_month = (
        db_session_spire.query(func.count(JournalEntry.id))
        .filter(
            JournalEntry.journal_id == humbug_integration.journal_id,
            func.DATE(JournalEntry.created_at) > start_of_month_timestamp,
        )
        .one_or_none()
    )
    if journal_data_month is None:
        return None

    group_data = (
        db_session_brood.query(Group.name, User.email)
        .join(GroupUser, GroupUser.group_id == Group.id)
        .join(User, User.id == GroupUser.user_id)
        .filter(
            Group.id == humbug_integration.group_id, GroupUser.user_type == Role.owner
        )
        .all()
    )
    group_subscriptions = (
        db_session_brood.query(Subscription)
        .join(
            SubscriptionPlan, SubscriptionPlan.id == Subscription.subscription_plan_id
        )
        .filter(
            Subscription.group_id == humbug_integration.group_id,
            Subscription.active == True,
            SubscriptionPlan.plan_type == SubscriptionPlanType.events.value,
        )
        .all()
    )
    kv_variable = (
        db_session_brood.query(KVBrood)
        .filter(KVBrood.kv_key == "BUGOUT_HUMBUG_MONTHLY_EVENTS_FREE_SUBSCRIPTION_PLAN")
        .one()
    )
    free_units_events = (
        db_session_brood.query(SubscriptionPlan.default_units)
        .filter(SubscriptionPlan.id == kv_variable.kv_value)
        .one()[0]
    )

    return GroupIntegrationData(
        integration_id=humbug_integration.id,
        group_id=humbug_integration.group_id,
        group_name=group_data[0][0],
        group_owners=[GroupOwner(email=owner[1]) for owner in group_data],
        num_events_total=journal_data[1],
        num_events_month=journal_data_month[0],
        available_events=sum([sub.units for sub in group_subscriptions]) * 1000,
        available_free_events=free_units_events * 1000,
    )


def send_billing_report_email(content: str, email: str) -> None:
    """
    Send reset password for given email.
    """
    message = Mail(
        from_email=BUGOUT_FROM_EMAIL,
        to_emails=email,
        subject="Billing for crash reports report",
        html_content=content,
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
    except Exception as err:
        logger.error(f"Error occured due sending billing report: {err}")
