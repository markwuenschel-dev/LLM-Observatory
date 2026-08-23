"""Authoritative runtime observation state.

Container health is not observation. A stack can report every service healthy
while the normalized store has accepted nothing for days -- that exact failure
ran undetected on a live host for thirteen days. This module answers the only
question that matters operationally: *is Observatory actually receiving and
persisting useful telemetry, and for which clients?*

Two verdicts are produced:

* a per-client runtime state -- OBSERVING, DEGRADED, CONFIGURED_UNVERIFIED,
  NOT_CONFIGURED, or UNSUPPORTED -- carrying last event time, project-attribution
  health, the signal families actually present, and the exact blockers; and
* a global OBSERVATION_CAPABLE verdict backed by evidence rather than uptime.

Every coverage grade is reported with the numerator and denominator that
produced it, so a client that emits nothing is never silently compared as though
it emitted zeroes.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping

from .clients import CLIENT_SPECS
from .clock import utc_now
from .contracts import ensure_utc

# Per-client runtime states.
OBSERVING = "OBSERVING"
DEGRADED = "DEGRADED"
CONFIGURED_UNVERIFIED = "CONFIGURED_UNVERIFIED"
NOT_CONFIGURED = "NOT_CONFIGURED"
UNSUPPORTED = "UNSUPPORTED"
CLIENT_STATES = (OBSERVING, DEGRADED, CONFIGURED_UNVERIFIED, NOT_CONFIGURED, UNSUPPORTED)

# Coverage grades for one client and one signal family.
COMPLETE = "COMPLETE"
PARTIAL = "PARTIAL"
UNKNOWN = "UNKNOWN"
COVERAGE_UNSUPPORTED = "UNSUPPORTED"
COVERAGE_GRADES = (COMPLETE, PARTIAL, UNKNOWN, COVERAGE_UNSUPPORTED)

# A family is COMPLETE only when nearly every observed event carries it; the
# ratio is always reported so the grade never stands alone.
COMPLETE_RATIO = 0.95

# How stale the newest event may be before a configured client is DEGRADED.
DEFAULT_FRESHNESS_SECONDS = 24 * 3600
# How much of a client's telemetry must resolve to a real project.
MIN_ATTRIBUTION_RATIO = 0.5
# Below this, a client does not meaningfully report sessions and no
# host-level binding can attribute it.
MIN_SESSION_RATIO = 0.1
# Beyond this much clock skew a timestamp is not evidence of freshness.
FUTURE_TIMESTAMP_TOLERANCE_SECONDS = 300
# Observation window for the coverage sample.
DEFAULT_WINDOW_SECONDS = 7 * 24 * 3600

# Signal families, in the order an operator reads them. Each maps to the column
# expression that decides whether one event carries that family; `EventStore`
# owns the SQL, this module owns the vocabulary and the grading.
SIGNAL_FAMILIES = (
    "model_identity",
    "session_identity",
    "agent_identity",
    "token_usage",
    "cost",
    "latency",
    "tool_calls",
    "errors_retries",
    "project_attribution",
    "outcomes",
    # Skill / workflow / task-class / lane. The objective ranks by these, so
    # their absence has to be as visible as any other coverage gap: measured
    # live at 4 of 42,577 cost-bearing events, no ranking on them can ever
    # emerge, and without a family reporting it that reads as "no results yet"
    # rather than "this dimension is not captured".
    "capability_identity",
)

# Clients that cannot be observed by host-level configuration alone. These are
# not failures; representing them as NOT_CONFIGURED would imply a fix exists.
_CALLER_OWNED_CONFIG_KINDS = frozenset({"discovery-only", "adapter-only"})


# A client is known by three names: the catalog key (`codex`), the name it is
# recorded under (`spec.name`), and the `service.name` its telemetry actually
# carries. Reporting those as separate rows would show one client twice -- once
# configured-but-silent and once observing -- which is exactly the confusion this
# module exists to remove. Canonical identity is `spec.name`.
_TELEMETRY_ALIASES = {
    # The Codex app server reports itself under its process name.
    "codex-app-server": "codex",
}


def _canonical(client_name: str) -> str:
    alias = _TELEMETRY_ALIASES.get(client_name, client_name)
    spec = CLIENT_SPECS.get(alias)
    if spec is not None:
        return spec.name
    for candidate in CLIENT_SPECS.values():
        if candidate.name == alias:
            return candidate.name
    return client_name


def _spec_for(client_name: str):
    """Resolve a spec by catalog key, recorded name, or telemetry alias."""

    alias = _TELEMETRY_ALIASES.get(client_name, client_name)
    spec = CLIENT_SPECS.get(alias)
    if spec is not None:
        return spec
    for candidate in CLIENT_SPECS.values():
        if candidate.name == alias:
            return candidate
    return None


def _merge_samples(samples: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Fold every alias of one client into a single canonical sample."""

    merged: dict[str, dict[str, Any]] = {}
    counters = ("events",) + SIGNAL_FAMILIES
    for raw_name, sample in samples.items():
        target = merged.setdefault(_canonical(raw_name), {"reported_as": []})
        target["reported_as"].append(raw_name)
        for key in counters:
            target[key] = int(target.get(key, 0) or 0) + int(sample.get(key, 0) or 0)
        for key in ("last_observed", "last_received"):
            value = sample.get(key)
            if value and (target.get(key) is None or str(value) > str(target[key])):
                target[key] = value
    return merged


def _grade(present: int, observed: int, *, supported: bool) -> dict[str, Any]:
    """Grade one signal family, always alongside its evidence."""

    if not supported:
        grade = COVERAGE_UNSUPPORTED
    elif observed <= 0:
        # No sample. Absence of evidence, not evidence of absence.
        grade = UNKNOWN
    elif present <= 0:
        # A sample, and the signal is simply not in it. This is NOT partial:
        # PARTIAL invites a reader to average it in as a low score, which is
        # exactly "comparing missing telemetry as though it were zero
        # telemetry". Nothing can be said about this dimension for this client,
        # so say that. Live case: capability_identity at 0 of 29,062 events for
        # claude-code -- graded PARTIAL, it read as "a bit of skill data";
        # graded UNKNOWN, it reads as "skills cannot be ranked here at all".
        grade = UNKNOWN
    elif present >= observed * COMPLETE_RATIO:
        grade = COMPLETE
    else:
        grade = PARTIAL
    return {
        "coverage": grade,
        "events_with_signal": present,
        "events_observed": observed,
        "ratio": (present / observed) if observed else None,
    }


def _timestamp_age(timestamp: str | None, now) -> tuple[float | None, float | None]:
    """Return `(age_seconds, future_skew_seconds)` for one timestamp.

    `received_at` is payload-supplied, so a client can stamp its telemetry ahead
    of the collector's clock. Clamping such a stamp to age 0 let one skewed
    event be elected "newest" for the whole deployment and suppressed the
    staleness blocker. Returning a bare `None` instead was the same false green
    by another route: the staleness check reads `age is not None and age >
    budget`, so a `None` age *skips* it, and the deployment-wide `min()` drops
    that client from the verdict entirely.

    So the two failure modes are reported separately. A stamp beyond the
    tolerance yields no age -- it is not evidence of anything -- and yields the
    skew, which the caller must state as a blocker rather than silently skip.
    """

    if not timestamp:
        return None, None
    try:
        observed = ensure_utc(timestamp, "observed_at")
    except Exception:
        return None, None
    elapsed = (now - observed).total_seconds()
    if elapsed < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
        return None, -elapsed
    return max(0.0, elapsed), None


def _freshness_evidence(candidates, now) -> tuple[float | None, Any, float | None, Any]:
    """Pick the freshness evidence from candidate stamps, preferred order first.

    Returns `(age, stamp, skew, skew_stamp)`. `stamp` is the timestamp the age
    was actually derived from, so the reported age and the reported time always
    describe the same event. `skew`/`skew_stamp` describe the worst future
    offset seen, whether or not a usable stamp was also found: a client whose
    `received_at` is an hour ahead has a clock problem even when its
    `observed_at` is sane, and that problem is exactly why its freshness cannot
    be taken at face value.
    """

    usable: tuple[float, Any] | None = None
    skew: float | None = None
    skew_stamp: Any = None
    fallback: Any = None
    for stamp in candidates:
        if not stamp:
            continue
        if fallback is None:
            fallback = stamp
        age, future = _timestamp_age(stamp, now)
        if future is not None:
            if skew is None or future > skew:
                skew, skew_stamp = future, stamp
            continue
        if age is not None and usable is None:
            usable = (age, stamp)
    if usable is not None:
        return usable[0], usable[1], skew, skew_stamp
    # Nothing usable: report the stamp the operator would otherwise see, with no
    # age at all, so the blocker -- not a silent None -- carries the verdict.
    return None, fallback, skew, skew_stamp


def _skew_blocker(skew: float, stamp: Any) -> str:
    return (
        f"newest telemetry timestamp is {skew / 60:.1f} minutes in the future "
        f"({str(stamp)[:19]}); freshness cannot be established from it -- check the "
        "clock on the host emitting this telemetry"
    )


def client_observation(
    client_name: str,
    sample: Mapping[str, Any] | None,
    *,
    configured: bool,
    recent: Mapping[str, Any] | None = None,
    lifetime: Mapping[str, Any] | None = None,
    now=None,
    freshness_seconds: int = DEFAULT_FRESHNESS_SECONDS,
) -> dict[str, Any]:
    """Return the authoritative runtime state for one client.

    `sample` is the store aggregate for this client, or None when the store has
    never seen it. `configured` reflects the installed ownership manifest, not a
    guess from telemetry: a client can be configured and silent, which is
    precisely the condition worth surfacing.
    """

    now = now or utc_now()
    spec = _spec_for(client_name)
    observed = int((sample or {}).get("events", 0) or 0)
    age, last_event, skew, skew_stamp = _freshness_evidence(
        ((sample or {}).get("last_received"), (sample or {}).get("last_observed")), now
    )

    host_observable = spec is not None and spec.config_kind not in _CALLER_OWNED_CONFIG_KINDS
    families: dict[str, Any] = {}
    for family in SIGNAL_FAMILIES:
        families[family] = _grade(
            int((sample or {}).get(family, 0) or 0),
            observed,
            supported=host_observable or observed > 0,
        )

    attributed = int((sample or {}).get("project_attribution", 0) or 0)
    window_ratio = (attributed / observed) if observed else None
    # Attribution health is a statement about now, not a historical average.
    # Stored events are immutable, so a window that reaches back over a period
    # when attribution was broken keeps reporting it as broken long after it
    # was fixed. Grade on recent telemetry and show both numbers.
    recent_sample = recent if recent is not None else sample
    recent_observed = int((recent_sample or {}).get("events", 0) or 0)
    recent_attributed = int((recent_sample or {}).get("project_attribution", 0) or 0)
    recent_ratio = (recent_attributed / recent_observed) if recent_observed else None
    attribution_ratio = recent_ratio if recent_ratio is not None else window_ratio

    blockers: list[str] = []
    lifetime_events = int((lifetime or {}).get("events", 0) or 0)
    # A caller-owned adapter that shipped for months and died is stale, not
    # "never instrumented". Checking UNSUPPORTED first discarded that evidence
    # for every discovery-only and adapter-only spec.
    if not host_observable and observed == 0 and lifetime_events == 0:
        state = UNSUPPORTED
        blockers.append(
            f"{client_name} has no host-level telemetry configuration; it is caller-owned "
            "and must be instrumented by the application that makes the API call"
        )
    elif observed == 0 and lifetime_events > 0:
        # Delivered before, silent now. This is a stale hook or a client that
        # stopped emitting -- a different problem from never having worked, and
        # telling the operator to reconfigure would send them the wrong way.
        state = DEGRADED
        seen_age, last_seen, skew, skew_stamp = _freshness_evidence(
            ((lifetime or {}).get("last_received"), (lifetime or {}).get("last_observed")), now
        )
        blockers.append(
            f"delivered {lifetime_events:,} events "
            f"(last {str(last_seen)[:19] if last_seen else 'unknown'}"
            + (f", {seen_age / 86400:.1f} days ago" if seen_age is not None else "")
            + ") but nothing within the observation window; the client may have stopped "
            "emitting or its hook may no longer be firing"
        )
        last_event = last_seen
        age = seen_age
        if skew is not None:
            blockers.append(_skew_blocker(skew, skew_stamp))
        if not configured:
            blockers.append(
                "this client is not in the installed ownership manifest, so "
                "`uninstall` cannot reverse whatever is configured for it"
            )
    elif not configured and observed == 0:
        # Checked only after the lifetime branch: a client that delivered for
        # months and then stopped is a stale hook, not an unconfigured one, and
        # telling the operator to reconfigure sends them the wrong way. The
        # manifest may simply have been lost or the client self-instrumented.
        state = NOT_CONFIGURED
        blockers.append(f"run `observatory configure {client_name} --apply` to enable host-level telemetry")
    elif observed == 0:
        state = CONFIGURED_UNVERIFIED
        blockers.append("configured, but no telemetry from this client has ever been persisted")
    else:
        state = OBSERVING
        if age is None and skew is None:
            # Events arrived but carry no usable timestamp at all -- absent, or
            # unparseable. The staleness check below then has nothing to test
            # and is silently skipped, which is the same false green as the
            # future-stamp case reached through a different input.
            state = DEGRADED
            blockers.append(
                "telemetry has arrived but carries no usable timestamp, so its "
                "freshness cannot be established"
            )
        if skew is not None:
            # An unusable timestamp is a stated problem, never a skipped check.
            # This client's telemetry may or may not be fresh -- the point is
            # that nothing here can establish that it is, so it must not be
            # reported as OBSERVING on the strength of a stamp from the future.
            state = DEGRADED
            blockers.append(_skew_blocker(skew, skew_stamp))
        if age is not None and age > freshness_seconds:
            state = DEGRADED
            blockers.append(
                f"newest event is {age / 3600:.1f}h old, beyond the {freshness_seconds / 3600:.0f}h freshness budget"
            )
        if attribution_ratio is not None and attribution_ratio < MIN_ATTRIBUTION_RATIO:
            state = DEGRADED
            blockers.append(
                f"only {attribution_ratio:.0%} of events resolve to a project; "
                "telemetry is arriving but cannot be attributed to a repository"
            )
            # Distinguish "not bound yet" from "can never be bound". A client
            # that emits no session, task, or worktree identifier offers nothing
            # for a host-level source to join against, so telling the operator
            # to run a discovery command would waste their time.
            sessions_seen = int((recent_sample or {}).get("session_identity", 0) or 0)
            # Presence is not coverage. A client emitting a session on 2 of
            # 345,000 events does not "have sessions" in any actionable sense,
            # and telling the operator to bind them wastes their time on a
            # client no host-level source can help.
            session_ratio = (sessions_seen / recent_observed) if recent_observed else 0.0
            if recent_observed and session_ratio < MIN_SESSION_RATIO:
                blockers.append(
                    f"{client_name} emits no session, task, or worktree identifier, so no "
                    "host-level source can bind its telemetry to a repository; attribution "
                    "requires the client itself to report a working directory or session"
                    + (
                        f" (only {sessions_seen:,} of {recent_observed:,} events carry one)"
                        if sessions_seen
                        else ""
                    )
                )
            elif sessions_seen:
                blockers.append(
                    "sessions are present but unbound; run `observatory bind-sessions --apply` "
                    "to record which project each session belongs to"
                )
        if not configured:
            # Telemetry without an ownership record: real, but unmanaged, so
            # `uninstall` cannot reverse whatever is producing it.
            blockers.append("telemetry is arriving but this client is not in the installed ownership manifest")

    return {
        "client": client_name,
        "provider": spec.provider if spec is not None else "unknown",
        "state": state,
        "configured": configured,
        "events_observed": observed,
        "last_event_at": last_event,
        "last_event_age_seconds": age,
        # Present so a consumer can tell "no age because nothing arrived" from
        # "no age because the newest stamp is unusable" without parsing prose.
        "clock_skew_seconds": skew,
        "project_attribution": {
            "attributed_events": recent_attributed if recent_ratio is not None else attributed,
            "observed_events": recent_observed if recent_ratio is not None else observed,
            "ratio": attribution_ratio,
            "window_ratio": window_ratio,
            "window_attributed_events": attributed,
            "window_observed_events": observed,
            "health": (
                UNKNOWN
                if attribution_ratio is None
                else COMPLETE
                if attribution_ratio >= COMPLETE_RATIO
                else PARTIAL
            ),
        },
        "signal_families": families,
        "reported_as": sorted((sample or {}).get("reported_as", []) or []),
        "blockers": blockers,
    }


def observation_report(
    samples: Mapping[str, Mapping[str, Any]],
    *,
    recent_samples: Mapping[str, Mapping[str, Any]] | None = None,
    lifetime_totals: Mapping[str, Mapping[str, Any]] | None = None,
    configured_clients: Mapping[str, Any] | None = None,
    store_health: Mapping[str, Any] | None = None,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    freshness_seconds: int = DEFAULT_FRESHNESS_SECONDS,
    now=None,
) -> dict[str, Any]:
    """Build the whole-deployment observation verdict.

    OBSERVATION_CAPABLE is deliberately hard to satisfy: it requires a reachable
    store with headroom *and* at least one client whose telemetry actually
    landed recently. A stack of healthy containers with an empty store is not
    capable of observation and must not report as though it were.
    """

    now = now or utc_now()
    configured_clients = configured_clients or {}
    store_health = store_health or {}

    merged = _merge_samples(samples)
    merged_recent = _merge_samples(recent_samples or {})
    lifetime_totals = _merge_samples(lifetime_totals or {})
    names = set(merged)
    names.update(_canonical(name) for name in configured_clients)
    names.update(spec.name for spec in CLIENT_SPECS.values())
    # The state vocabulary describes *clients*. Telemetry with no recognized
    # client identity is real and is reported, but it does not get a state --
    # calling it OBSERVING would let one unattributed record stand in for a
    # working client.
    recognized = sorted(name for name in names if _spec_for(name) is not None)
    unrecognized = sorted(name for name in names if _spec_for(name) is None)
    clients = [
        client_observation(
            name,
            merged.get(name),
            configured=_is_configured(name, configured_clients),
            recent=merged_recent.get(name),
            lifetime=(lifetime_totals or {}).get(name),
            now=now,
            freshness_seconds=freshness_seconds,
        )
        for name in recognized
    ]
    unidentified = [
        {"client": name, "events_observed": int(merged.get(name, {}).get("events", 0) or 0)}
        for name in unrecognized
        if int(merged.get(name, {}).get("events", 0) or 0) > 0
    ]

    # Telemetry that carries no client identity is real, but it is not a client.
    # Counting it as one lets a single unattributed record satisfy the global
    # verdict while every recognized client is degraded -- precisely the
    # false-green this module exists to prevent.
    observing = [c for c in clients if c["state"] == OBSERVING]
    degraded = [c for c in clients if c["state"] == DEGRADED]
    unidentified_events = sum(int(entry["events_observed"]) for entry in unidentified)

    blockers: list[str] = []
    store_reachable = bool(store_health.get("reachable", True))
    if not store_reachable:
        blockers.append("normalized store is not reachable; nothing can be persisted")
    if store_health.get("exhausted"):
        blockers.append(
            "normalized store is at its byte budget; intake is rejecting telemetry "
            "(raise `storage.max_database_bytes` or run `retention --enforce`)"
        )
    if not observing and not degraded:
        blockers.append("no configured client has ever delivered persisted telemetry")
        if unidentified_events:
            blockers.append(
                f"{unidentified_events} events arrived without a recognized client identity; "
                "they cannot establish that any client is being observed"
            )
    elif not observing:
        blockers.append("every client with telemetry is degraded; none is currently observing")

    # These are *ages*, so the newest telemetry is the smallest value. Taking
    # max() picked the least recently active client and then asserted "no client
    # has delivered", flipping the whole deployment to NOT_OBSERVATION_CAPABLE
    # because one client went quiet -- a permanent false red, which trains an
    # operator to ignore the verdict just as surely as a false green.
    newest = min(
        (c["last_event_age_seconds"] for c in clients if c["last_event_age_seconds"] is not None),
        default=None,
    )
    if newest is not None and newest > freshness_seconds:
        blockers.append(
            f"no client has delivered telemetry in {newest / 3600:.1f}h; "
            "the deployment looks alive but is not observing"
        )
    # A client excluded from `newest` because its stamps are unusable must not
    # be excluded from the verdict too. When skew is the *only* reason the
    # deployment has no freshness evidence, say so instead of returning capable
    # on the strength of a `min()` that quietly had nothing to compare.
    skewed = [c for c in clients if c.get("clock_skew_seconds") is not None]
    if skewed and newest is None:
        blockers.append(
            "the newest telemetry timestamp in the deployment is in the future ("
            + ", ".join(sorted(c["client"] for c in skewed))
            + "); no client's freshness can be established -- check host clock skew"
        )

    capable = not blockers
    return {
        "schema": "observatory.observation/v1",
        "generated_at": now.isoformat(),
        "window_seconds": window_seconds,
        "observation_capable": capable,
        "verdict": "OBSERVATION_CAPABLE" if capable else "NOT_OBSERVATION_CAPABLE",
        "evidence": {
            "clients_observing": len(observing),
            "clients_degraded": len(degraded),
            "unidentified_client_events": unidentified_events,
            "newest_event_age_seconds": newest,
            "store_reachable": store_reachable,
            "store_capacity_ratio": store_health.get("ratio"),
            "events_in_window": sum(int(c["events_observed"]) for c in clients),
        },
        "blockers": blockers,
        "clients": clients,
        "unidentified_clients": unidentified,
    }


def _is_configured(client_name: str, configured_clients: Mapping[str, Any]) -> bool:
    """Ownership is recorded under the catalog key, telemetry under the spec name."""

    canonical = _canonical(client_name)
    for key, record in configured_clients.items():
        if _canonical(key) == canonical:
            return _applied(record)
    return False


def _applied(record: Any) -> bool:
    if isinstance(record, Mapping):
        return bool(record.get("applied"))
    return bool(record)


def window_start(window_seconds: int, now=None) -> str:
    now = now or utc_now()
    return (now - timedelta(seconds=window_seconds)).isoformat()
