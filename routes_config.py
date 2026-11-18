"""Target members, repository mappings and settings."""

from fastapi import APIRouter, Depends, Response
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.api_dependencies import get_services, get_session
from app.api.api_errors import ApiError
from app.core.app_services import AppServices
from app.models.tracking_models import PullRequest, Repository, TargetMember
from app.schemas.api_schemas import MemberIn, MemberOut, MemberPatch, RepoIn, RepoOut, RepoPatch, SettingsOut
from app.services.command_template import ALLOWED_PLACEHOLDERS
from app.services.repo_path_validator import validate_repo_path
from app.services.settings_service import (
    EffectiveSettings,
    SettingsValidationError,
    get_effective_settings,
    update_settings,
)

router = APIRouter(prefix="/api")


async def _commit_unique(session: AsyncSession, what: str) -> None:
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ApiError(409, "duplicate", f"{what} đã tồn tại") from exc


async def _get_or_404(session: AsyncSession, model, item_id: int, what: str):
    item = await session.get(model, item_id)
    if item is None:
        raise ApiError(404, "not_found", f"Không tìm thấy {what} {item_id}")
    return item


# ---- members ---------------------------------------------------------------------------------

@router.get("/members", response_model=list[MemberOut])
async def list_members(session: AsyncSession = Depends(get_session)):
    return (await session.scalars(select(TargetMember).order_by(TargetMember.username))).all()


@router.post("/members", response_model=MemberOut, status_code=201)
async def create_member(body: MemberIn, session: AsyncSession = Depends(get_session)):
    member = TargetMember(**body.model_dump())
    session.add(member)
    await _commit_unique(session, f"Member '{body.username}'")
    return member


@router.patch("/members/{member_id}", response_model=MemberOut)
async def update_member(member_id: int, body: MemberPatch, session: AsyncSession = Depends(get_session)):
    member = await _get_or_404(session, TargetMember, member_id, "member")
    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(member, key, value)
    await session.commit()
    return member


@router.delete("/members/{member_id}", status_code=204)
async def delete_member(member_id: int, session: AsyncSession = Depends(get_session)) -> Response:
    await session.delete(await _get_or_404(session, TargetMember, member_id, "member"))
    await session.commit()
    return Response(status_code=204)


# ---- repositories ----------------------------------------------------------------------------

async def _repo_out(repo: Repository) -> RepoOut:
    problems = await validate_repo_path(repo.local_path, repo.full_name)
    return RepoOut(id=repo.id, provider=repo.provider, full_name=repo.full_name, local_path=repo.local_path,
                   enabled=repo.enabled, path_valid=not problems, path_problems=problems)


@router.get("/repos", response_model=list[RepoOut])
async def list_repos(session: AsyncSession = Depends(get_session)):
    repos = (await session.scalars(select(Repository).order_by(Repository.full_name))).all()
    return [await _repo_out(repo) for repo in repos]


@router.post("/repos", response_model=RepoOut, status_code=201)
async def create_repo(body: RepoIn, session: AsyncSession = Depends(get_session)):
    repo = Repository(**body.model_dump())
    session.add(repo)
    await _commit_unique(session, f"Repo '{body.full_name}'")
    return await _repo_out(repo)


@router.patch("/repos/{repo_id}", response_model=RepoOut)
async def update_repo(repo_id: int, body: RepoPatch, session: AsyncSession = Depends(get_session)):
    repo = await _get_or_404(session, Repository, repo_id, "repo")
    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(repo, key, value)
    await session.commit()
    return await _repo_out(repo)


@router.delete("/repos/{repo_id}", status_code=204)
async def delete_repo(repo_id: int, session: AsyncSession = Depends(get_session)) -> Response:
    repo = await _get_or_404(session, Repository, repo_id, "repo")
    locked = await session.scalar(select(PullRequest.id).where(
        PullRequest.repository_id == repo_id, PullRequest.is_locked.is_(True)).limit(1))
    if locked is not None:
        raise ApiError(409, "repo_busy", "Repo đang có PR được AI review, đợi chạy xong rồi xóa")
    await session.delete(repo)
    await session.commit()
    return Response(status_code=204)


# ---- settings --------------------------------------------------------------------------------

def _settings_out(eff: EffectiveSettings) -> SettingsOut:
    return SettingsOut(
        reviewer_username=eff.reviewer_username, reviewer_username_source=eff.reviewer_username_source,
        cli_command_template=eff.cli_command_template, default_cli_command_template=eff.default_cli_command_template,
        allowed_placeholders=sorted(ALLOWED_PLACEHOLDERS), auto_run_enabled=eff.auto_run_enabled,
        poll_interval_seconds=eff.poll_interval_seconds, run_timeout_seconds=eff.run_timeout_seconds,
        auto_run_quiet_minutes=eff.auto_run_quiet_minutes, max_auto_runs_per_hour=eff.max_auto_runs_per_hour,
        github_token_configured=eff.github_token_configured,
    )


@router.get("/settings", response_model=SettingsOut)
async def read_settings(session: AsyncSession = Depends(get_session), services: AppServices = Depends(get_services)):
    return _settings_out(await get_effective_settings(session, services.env))


@router.patch("/settings", response_model=SettingsOut)
async def patch_settings(patch: dict, session: AsyncSession = Depends(get_session),
                         services: AppServices = Depends(get_services)):
    try:
        eff = await update_settings(session, services.env, patch)
    except SettingsValidationError as exc:
        raise ApiError(422, "invalid_settings", str(exc)) from exc
    if "poll_interval_seconds" in patch:
        services.scheduler.reschedule(eff.poll_interval_seconds)
    return _settings_out(eff)
