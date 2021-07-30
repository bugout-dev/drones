from datetime import datetime
from enum import Enum
from os import error
from typing import List, Optional, Dict, Any
from uuid import UUID
from spire.humbug.data import HumbugCreateReportTask, HumbugReport

from pydantic import BaseModel, Field


class StatsTypes(Enum):
    errors = "errors"
    stats = "stats"
    session = "session"
    client = "client"


class TimeScales(Enum):
    year = "year"
    month = "month"
    week = "week"
    day = "day"


class RedisPickCommand(Enum):
    lpop = "lpop"
    lrange = "lrange"


class GroupOwner(BaseModel):
    email: str


class GroupIntegrationData(BaseModel):
    integration_id: UUID
    group_id: UUID
    group_name: str
    group_owners: List[GroupOwner] = Field(default_factory=list)
    num_events_total: int
    num_events_month: int
    available_events: int
    available_free_events: int


class UpdateStatsRequest(BaseModel):
    journal_id: UUID
    stats_version: int
    stats_type: List[str] = []
    timescale: List[str] = []
    push_to_bucket: Optional[bool] = True


class Jobs(BaseModel):
    job_type: str
    stats_update: Optional[UpdateStatsRequest]


class UpdateStatsResponce(BaseModel):
    modified_since: datetime
    journal_statistics: Optional[List[Dict[str, Any]]] = None


class HumbugFailedReportTask(BaseModel):
    report: HumbugReport
    reported_at: Optional[datetime]
    bugout_token: UUID
    error: Optional[str] = None
