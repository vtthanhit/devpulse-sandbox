"""Run detail and live output over SSE.

Event data is JSON-encoded: raw text chunks ending in a newline would otherwise lose it when the SSE
framing splits data into `data:` lines.
"""

import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from app.api.api_dependencies import get_services, get_session
from app.api.api_errors import ApiError
from app.core.app_services import AppServices
from app.models.pr_status import RunStatus
from app.models.tracking_models import ReviewRunLog
from app.schemas.api_schemas import RunDetailOut

router = APIRouter(prefix="/api")
PING_SECONDS = 15


async def _get_run(session: AsyncSession, run_id: int) -> ReviewRunLog:
    run = await session.get(ReviewRunLog, run_id)
    if run is None:
        raise ApiError(404, "run_not_found", f"Không tìm thấy run {run_id}")
    return run


@router.get("/runs/{run_id}", response_model=RunDetailOut)
async def run_detail(run_id: int, session: AsyncSession = Depends(get_session)):
    run = await _get_run(session, run_id)
    return RunDetailOut(
        id=run.id, pr_id=run.pr_id, round_number=run.round_number, status=run.status, trigger=run.trigger,
        exit_code=run.exit_code, head_sha=run.head_sha, started_at=run.started_at, finished_at=run.finished_at,
        command=json.loads(run.command or "[]"), raw_output=run.raw_output,
    )


def _event(name: str, data: str) -> dict:
    return {"event": name, "data": json.dumps(data)}


@router.get("/runs/{run_id}/stream")
async def run_stream(run_id: int, request: Request, session: AsyncSession = Depends(get_session),
                     services: AppServices = Depends(get_services)):
    await _get_run(session, run_id)
    live = services.broker.subscribe(run_id)

    async def finished_from_db() -> AsyncIterator[dict]:
        # Re-read in a fresh session: the run may have finished after the first read.
        async with services.session_factory() as fresh:
            run = await _get_run(fresh, run_id)
        if run.raw_output:
            yield _event("output", run.raw_output)
        yield _event("status", run.status if run.status != RunStatus.RUNNING else RunStatus.FAILED)
        yield _event("end", "")

    async def live_events() -> AsyncIterator[dict]:
        assert live is not None
        async for name, data in live:
            if await request.is_disconnected():
                break
            yield _event(name, data)

    return EventSourceResponse(live_events() if live is not None else finished_from_db(), ping=PING_SECONDS)
