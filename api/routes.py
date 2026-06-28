from fastapi import APIRouter
from sqlalchemy import text

from api.database import engine

router = APIRouter()


@router.get("/")
def home():

    return {
        "message": "Real Time Log Monitoring API"
    }


@router.get("/access-logs")
def access_logs(limit: int = 100):

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM access_logs
                ORDER BY id DESC
                LIMIT :limit
                """
            ),
            {"limit": limit}
        )

        return [dict(row._mapping) for row in rows]


@router.get("/error-logs")
def error_logs():

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM error_logs
                ORDER BY id DESC
                """
            )
        )

        return [dict(row._mapping) for row in rows]


@router.get("/top-ips")
def top_ips():

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM top_ips
                ORDER BY request_count DESC
                """
            )
        )

        return [dict(row._mapping) for row in rows]


@router.get("/top-endpoints")
def top_endpoints():

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM top_endpoints
                ORDER BY request_count DESC
                """
            )
        )

        return [dict(row._mapping) for row in rows]


@router.get("/requests-per-minute")
def requests_per_minute():

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM requests_per_minute
                ORDER BY window_start DESC
                """
            )
        )

        return [dict(row._mapping) for row in rows]


@router.get("/errors-per-minute")
def errors_per_minute():

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM errors_per_minute
                ORDER BY window_start DESC
                """
            )
        )

        return [dict(row._mapping) for row in rows]


@router.get("/alerts")
def alerts():

    with engine.connect() as conn:

        rows = conn.execute(
            text(
                """
                SELECT *
                FROM alerts
                ORDER BY alert_time DESC
                """
            )
        )

        return [dict(row._mapping) for row in rows]