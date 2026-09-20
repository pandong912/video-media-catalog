"""Guarded execution boundary for immutable source-removal plans."""

from __future__ import annotations

from collections.abc import Callable

from video_media_catalog.community_release import (
    RemovalAction,
    RemovalReceiptStatus,
    RestrictedPurgeTarget,
    SourceRemovalPlan,
    SourceRemovalReceipt,
    build_source_removal_receipt,
)
from video_media_catalog.rights import RightsTerminationFence

FenceInstaller = Callable[[RightsTerminationFence], None]
TargetPurger = Callable[[RestrictedPurgeTarget], None]
GoldRebuilder = Callable[[SourceRemovalPlan], str]
IndexRebuilder = Callable[[SourceRemovalPlan, str | None], str]


def execute_source_removal(
    plan: SourceRemovalPlan,
    *,
    confirm_source_product_id: str,
    install_rights_fence: FenceInstaller,
    purge_target: TargetPurger,
    rebuild_gold: GoldRebuilder,
    rebuild_index: IndexRebuilder,
    executed_at: str,
) -> SourceRemovalReceipt:
    """Execute only an explicitly confirmed, allowlisted non-dry-run plan."""

    if plan.dry_run:
        raise ValueError("dry-run removal plan cannot be executed")
    if confirm_source_product_id != plan.rights_fence.source_product_id:
        raise ValueError("source removal confirmation does not match the plan")

    purged_target_ids: list[str] = []
    errors: list[str] = []
    rights_fence_installed = False
    re_gold_release_plan_id = None
    re_index_build_id = None
    try:
        install_rights_fence(plan.rights_fence)
        rights_fence_installed = True
    except Exception as exc:
        errors.append(f"RIGHTS_FENCE:{type(exc).__name__}:{exc}")
    if not errors and RemovalAction.PURGE_RESTRICTED in plan.actions:
        for target in plan.purge_targets:
            try:
                purge_target(target)
                purged_target_ids.append(target.target_id)
            except Exception as exc:
                errors.append(f"PURGE:{target.target_id}:{type(exc).__name__}:{exc}")
    if not errors and RemovalAction.RE_GOLD in plan.actions:
        try:
            re_gold_release_plan_id = rebuild_gold(plan)
        except Exception as exc:
            errors.append(f"RE_GOLD:{type(exc).__name__}:{exc}")
    if not errors and RemovalAction.RE_INDEX in plan.actions:
        try:
            re_index_build_id = rebuild_index(plan, re_gold_release_plan_id)
        except Exception as exc:
            errors.append(f"RE_INDEX:{type(exc).__name__}:{exc}")

    if not errors:
        status = RemovalReceiptStatus.COMPLETED
    elif (
        rights_fence_installed
        or purged_target_ids
        or re_gold_release_plan_id
        or re_index_build_id
    ):
        status = RemovalReceiptStatus.PARTIAL
    else:
        status = RemovalReceiptStatus.FAILED
    return build_source_removal_receipt(
        plan=plan,
        status=status,
        rights_fence_installed=rights_fence_installed,
        purged_target_ids=tuple(purged_target_ids),
        re_gold_release_plan_id=re_gold_release_plan_id,
        re_index_build_id=re_index_build_id,
        errors=tuple(errors),
        executed_at=executed_at,
    )
