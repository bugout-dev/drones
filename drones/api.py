import logging
from typing import Dict, Any, List
from datetime import datetime

import boto3
from fastapi import (
    FastAPI,
    BackgroundTasks,
    HTTPException,
)
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from . import statistics
from .data import StatsTypes, TimeScales, UpdateStatsResponce, UpdateStatsRequest
from .middleware import DronesAuthMiddleware
from .settings import (
    DRONES_BUCKET,
    DRONES_BUCKET_STATISTICS_PREFIX,
    STATISTICS_S3_PRESIGNED_URL_EXPIRATION_TIME,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(openapi_url=None)

app.add_middleware(DronesAuthMiddleware)


@app.get("/ping")
async def ping() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs/stats_update")
async def generate_stats_and_return_url(
    stats_update: UpdateStatsRequest, background_tasks: BackgroundTasks
) -> JSONResponse:
    """
    Generate and push to S3 bucket statistics.
    """
    s3_version_prefix = "v5/"
    if stats_update.stats_version != 5:
        raise HTTPException(400, f"Invalid stats_version={stats_update.stats_version}")

    last_modified_since = datetime.utcnow()

    available_types = [stats_type.value for stats_type in StatsTypes]
    available_timescales = [stats_type.value for stats_type in TimeScales]

    stats_types = [
        stats_type
        for stats_type in stats_update.stats_type
        if stats_type in available_types
    ]
    timescales = [
        timescale
        for timescale in stats_update.timescale
        if timescale in available_timescales
    ]

    stats_urls: List[Dict[str, Any]] = []

    s3_client = boto3.client("s3")

    for timescale in timescales:
        for stats_file in stats_types:
            # Generate link to S3 buket
            try:
                result_key = f"{DRONES_BUCKET_STATISTICS_PREFIX}/{stats_update.journal_id}/{s3_version_prefix}{timescale}/{stats_file}.json"
                stats_presigned_url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": DRONES_BUCKET, "Key": result_key},
                    ExpiresIn=STATISTICS_S3_PRESIGNED_URL_EXPIRATION_TIME,
                    HttpMethod="GET",
                )
                stats_urls.append(
                    {
                        "timescale": timescale,
                        "stats_type": stats_file,
                        "stats_url": stats_presigned_url,
                    }
                )
            except Exception as err:
                logger.warning(
                    f"Can't generate S3 presigned url in stats endpoint for Bucket:{DRONES_BUCKET}, Key:{result_key} get error:{err}"
                )
    try:
        background_tasks.add_task(
            statistics.generate_and_push_in_own_process,
            journal_id=stats_update.journal_id,
            stats_types=stats_types,
            timescales=timescales,
            push_to_bucket=stats_update.push_to_bucket,
        )
    except Exception as err:
        logger.error(
            f"Unexpected error occurred due create generation statistics task: {str(err)}"
        )
        raise HTTPException(status_code=500)

    return JSONResponse(
        status_code=200,
        content=jsonable_encoder(
            UpdateStatsResponce(
                journal_statistics=stats_urls, modified_since=last_modified_since
            ).dict()
        ),
    )
