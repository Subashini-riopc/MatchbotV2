"""Matcher-type -> SQL-fragment registry.

Mirrors matching/base.py's @register_matcher pattern (type name -> class),
but each registered generator emits SQL instead of instantiating a Python
object that runs a per-record loop. Adding a new matcher type later (e.g.
fuzzy/Levenshtein, nearest-neighbor) means writing one new generator module
and registering it here — zero change to cascade_builder.py or anything
that calls build_sql_fragments().

Only "deterministic" is implemented for this demo (see matchers/
deterministic.py) — the 4 matchers in config/global.yaml's chain today
(deterministic_external_id, deterministic_ssn, deterministic_name_dob,
deterministic_name_addr) are all this one type.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from matchbot_snowflake.config_models import MatcherSpec


@dataclass(slots=True, frozen=True)
class MatcherSqlFragment:
    """One matcher's contribution to the cascade query.

    Attributes
    ----------
    name:
        The matcher's name from config/global.yaml (e.g.
        'deterministic_external_id') — carried through for logging/debugging,
        not used in the generated SQL itself.
    priority:
        1-based position in the resolved matcher chain — lower wins.
        Assigned by cascade_builder.py from the chain's declared order,
        never hand-numbered here.
    join_predicate_sql:
        The ON-clause condition joining a staged row (aliased ``s``) to a
        candidate reference row (aliased ``r``) for this matcher. For a
        deterministic matcher this is the full exact-equality condition; for
        a fuzzy matcher this is the (cheap, selective) narrowing condition
        used to avoid a full cross join — see matchers/fuzzy.py — not the
        fuzzy comparison itself, which lives in score_sql instead.
    guard_predicate_sql:
        A WHERE-clause condition that must hold for the staged row before
        this matcher's join is considered at all — mirrors
        DeterministicMatcher.match()'s "any missing/blank key -> no match"
        short-circuit, evaluated once per staged row rather than
        implicitly through the join.
    method_label:
        The value written to rilds_matched.match_method / vocab.py's
        method_to_db() output for this matcher (e.g. 'EXACT_SASID', 'EXACT').
    score_sql:
        A SQL expression (evaluated per stage/candidate row pair, alias
        ``s``/``r`` in scope) producing this pair's match score in [0, 1].
        Deterministic matchers hardcode ``'1.0'`` (every join match is a
        certain accept, no partial credit). Fuzzy matchers compute a real
        weighted score here — see matchers/fuzzy.py.
    accept_threshold / review_threshold:
        Score bands for fuzzy matchers: score >= accept_threshold is a
        confirmed match; review_threshold <= score < accept_threshold is
        flagged for manual review (RILDS_ERROR, decision=LOW_CONFIDENCE);
        below review_threshold is not a candidate at all. Deterministic
        matchers use 1.0/1.0 (score is always exactly 1.0 or the row never
        appears as a candidate, since the join is exact-equality only).
    """

    name: str
    priority: int
    join_predicate_sql: str
    guard_predicate_sql: str
    method_label: str
    score_sql: str = "1.0"
    accept_threshold: float = 1.0
    review_threshold: float = 1.0


@dataclass(slots=True, frozen=True)
class _BuiltFragment:
    """What a registered builder returns for one MatcherSpec — everything
    MatcherSqlFragment needs except ``name``/``priority``, which
    build_sql_fragments fills in from the spec/chain position itself.
    score_sql/accept_threshold/review_threshold default to the
    always-1.0/always-exact values so existing deterministic builders (which
    only ever produce a certain accept-or-nothing join) don't need to name
    them explicitly.
    """

    join_predicate_sql: str
    guard_predicate_sql: str
    method_label: str
    score_sql: str = "1.0"
    accept_threshold: float = 1.0
    review_threshold: float = 1.0


_SQL_REGISTRY: dict[str, "_FragmentBuilder"] = {}

# A registered builder takes a MatcherSpec plus the current provider's
# external_id_column (e.g. 'sasid' for RIDE, 'ccri_id' for another provider
# — see ProviderConfig.external_id_column) and returns a _BuiltFragment for
# ONE matcher. external_id_column is needed because the generic 'rilds_id'
# key only exists as a real, same-named column on the STAGE side
# (rilds_stage.rilds_id, populated generically by
# provider_sql.py/cleanse.py); on the REFERENCE side there is no rilds_id
# column at all — the real Postgres pipeline resolves it dynamically per
# provider (storage/postgres.py: d["rilds_id"] = d.get(external_id_column)),
# and this SQL path must do the same rather than hardcode r.rilds_id
# (confirmed via a live SQL compilation error: 'invalid identifier
# R.RILDS_ID' against RILDS_REFERENCE, which only has a SASID column).
# Priority is assigned separately by build_sql_fragments, since it depends
# on chain position, not the spec alone.
_FragmentBuilder = Callable[["MatcherSpec", str], _BuiltFragment]


def register_sql_matcher(type_name: str) -> Callable[[_FragmentBuilder], _FragmentBuilder]:
    """Register a MatcherSpec.type -> SQL-fragment-builder function."""

    def _wrap(fn: _FragmentBuilder) -> _FragmentBuilder:
        if type_name in _SQL_REGISTRY:
            raise ValueError(f"SQL matcher type {type_name!r} already registered")
        _SQL_REGISTRY[type_name] = fn
        return fn

    return _wrap


def build_sql_fragments(
    specs: list["MatcherSpec"], external_id_column: str
) -> list[MatcherSqlFragment]:
    """Build one MatcherSqlFragment per enabled spec, in chain order.

    Mirrors matching/base.py's build_matchers(): only enabled specs are
    included, and priority is the 1-based position among those enabled
    specs (not the position in the full unfiltered list), matching how
    the Python cascade only ever considers enabled matchers.

    ``external_id_column`` is the current provider's
    ProviderConfig.external_id_column (e.g. 'sasid') — passed through to
    each builder so a 'rilds_id' key resolves to the right REFERENCE-side
    column (see _FragmentBuilder's comment above for why this can't be
    hardcoded).
    """
    # Import for side-effect registration of built-in SQL matcher types,
    # same lazy-import-for-registration pattern as matching/base.py.
    from matchbot_snowflake.matchers import deterministic  # noqa: F401
    from matchbot_snowflake.matchers import fuzzy  # noqa: F401

    fragments: list[MatcherSqlFragment] = []
    priority = 0
    for spec in specs:
        if not spec.enabled:
            continue
        priority += 1
        try:
            builder = _SQL_REGISTRY[spec.type]
        except KeyError:
            known = ", ".join(sorted(_SQL_REGISTRY)) or "<none>"
            raise KeyError(
                f"Unknown SQL matcher type {spec.type!r} for {spec.name!r}. "
                f"Registered types: {known}"
            ) from None
        built = builder(spec, external_id_column)
        fragments.append(
            MatcherSqlFragment(
                name=spec.name,
                priority=priority,
                join_predicate_sql=built.join_predicate_sql,
                guard_predicate_sql=built.guard_predicate_sql,
                method_label=built.method_label,
                score_sql=built.score_sql,
                accept_threshold=built.accept_threshold,
                review_threshold=built.review_threshold,
            )
        )
    return fragments
