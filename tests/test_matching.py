"""Unit tests for blocking and the matcher chain (RIDE: SASID-only matching)."""

from __future__ import annotations

from typing import Any

from matchbot.config.models import AppConfig
from matchbot.domain.enums import MatchDecision
from matchbot.matching import blocking
from matchbot.matching.base import build_matchers
from matchbot.pipeline.match import filter_chain_by_provider_attributes


def _with_rilds_id(reference_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mirror PostgresRepository/InMemoryRepository.load_reference()'s
    external_id_column -> rilds_id aliasing. rilds_reference rows only ever
    have ``sasid`` natively; blocking/matching key on the generic ``rilds_id``
    name, so tests must apply the same aliasing the real repositories do
    rather than pass raw fixture rows straight through."""
    out = []
    for r in reference_rows:
        d = dict(r)
        d.setdefault("rilds_id", d.get("sasid"))
        out.append(d)
    return out


def _run_chain(config: AppConfig, record: dict[str, Any], reference_rows: list[dict[str, Any]]):
    g = config.global_config
    provider = config.provider("ride_enrollment")
    keys = g.matching.blocking_keys
    candidates = _with_rilds_id(reference_rows)
    index = blocking.index_members(candidates, keys)
    mapped_attributes = set(provider.column_mappings.values())
    chain = filter_chain_by_provider_attributes(g.matching.matchers, mapped_attributes)
    matchers = build_matchers(chain, g.standardization)
    cands = [candidates[i] for i in blocking.candidate_indices(record, keys, index)]
    for m in matchers:
        out = m.match(record, cands)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            return out
    return None


def test_deterministic_sasid_match(
    app_config: AppConfig, ride_reference_rows: list[dict[str, Any]]
) -> None:
    rec = {"first_name": "MARY", "last_name": "CONTRERAS", "rilds_id": "1000049302"}
    out = _run_chain(app_config, rec, ride_reference_rows)
    assert out is not None
    assert out.decision is MatchDecision.MATCHED
    assert out.idcol_id == "1"
    assert out.score == 1.0


def test_no_match_for_new_person(
    app_config: AppConfig, ride_reference_rows: list[dict[str, Any]]
) -> None:
    rec = {"first_name": "ZELDA", "last_name": "NOBODY", "rilds_id": "9999999999"}
    out = _run_chain(app_config, rec, ride_reference_rows)
    assert out is None  # routed to UNMATCHED by the orchestrator


def test_blocking_narrows_candidates(
    app_config: AppConfig, ride_reference_rows: list[dict[str, Any]]
) -> None:
    keys = app_config.global_config.matching.blocking_keys
    candidates = _with_rilds_id(ride_reference_rows)
    index = blocking.index_members(candidates, keys)
    rec = {"rilds_id": "1000049302", "last_name": "CONTRERAS"}
    cand = blocking.candidate_indices(rec, keys, index)
    assert cand == [0]  # only reference row 1 shares a blocking key


def test_block_value_incomplete_returns_none(app_config: AppConfig) -> None:
    key = next(
        k for k in app_config.global_config.matching.blocking_keys if len(k.attributes) > 1
    )
    # Missing one of the key's attributes -> no blocking value.
    assert blocking.block_value({}, key) is None


def test_ride_chain_filters_to_sasid_only(app_config: AppConfig) -> None:
    """RIDE maps no ssn/birth_date/address1 — those rules must be dropped,
    leaving only deterministic_external_id in the resolved chain."""
    g = app_config.global_config
    provider = app_config.provider("ride_enrollment")
    mapped_attributes = set(provider.column_mappings.values())
    chain = filter_chain_by_provider_attributes(g.matching.matchers, mapped_attributes)
    assert [m.name for m in chain] == ["deterministic_external_id"]


def test_full_chain_prefers_strict_exact_over_fuzzy(app_config: AppConfig) -> None:
    """A record that clears an exact address tier must be matched by that
    exact rule, never fall through to the (looser, lower-priority) fuzzy
    tier — the core safety property of the strict-to-loose cascade."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "ROBERT", "last_name_std": "SMITH",
        "address1_std": "123 MAIN ST", "city": "PROVIDENCE", "state": "RI", "zip5": "02901",
    }
    # A candidate that matches exactly on the full-address tier (rule 5)
    # AND would also score well under the fuzzy tiers below it — the exact
    # rule must win since it's tried first.
    candidates = [{
        "idcol_id": "1",
        "first_name_std": "ROBERT", "last_name_std": "SMITH",
        "address1_std": "123 MAIN ST", "city": "PROVIDENCE", "state": "RI", "zip5": "02901",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            assert m.name == "deterministic_name_addr_full"
            assert out.decision is MatchDecision.MATCHED
            assert out.score == 1.0
            return
    raise AssertionError("expected the full-address exact tier to match")


def test_fuzzy_exact_addr_tier_reached_when_names_and_address_are_close_but_not_exact(
    app_config: AppConfig,
) -> None:
    """A record with an exact zip5 match, a non-exact address1_std, and a
    close-but-not-exact first name must fall through every exact tier
    (none of which tolerate a non-exact field) to fuzzy_name_exact_addr
    (rule 10) — and land in the review band there, since the failed
    address1_std comparison brings its weighted score (0.667) below that
    rule's own 0.8 accept_threshold but above its 0.6 review_threshold."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE APT 2B", "zip5": "02909",
    }
    candidates = [{
        "idcol_id": "7734",
        "first_name_std": "KATRINA", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE UNIT 2", "zip5": "02909",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            assert m.name == "fuzzy_name_exact_addr"
            assert out.decision is MatchDecision.AMBIGUOUS
            assert 0.6 <= out.score < 0.8
            return
    raise AssertionError("expected fuzzy_name_exact_addr to flag this record for review")


def test_fuzzy_combined_tier_reached_when_no_field_is_exact(app_config: AppConfig) -> None:
    """A record with a dissimilar address (jaro_winkler ~0.52, clears
    neither rule 10's nor rule 11's 0.8 address threshold) and a differing
    zip5 must fall through every exact tier AND both partially-exact fuzzy
    tiers (10/11 each score too low even for their own review_threshold,
    since neither retains a full exact/high-scoring side) to reach
    fuzzy_name_addr_combined (rule 12) — landing in the review band there:
    last_name/birth_date/ssn4 agreement (60/115 = 0.696) is real
    corroborating evidence, but not enough on its own, given the address
    comparison failed outright, to auto-accept."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE APT 2B", "zip5": "02909",
        "birth_date": "1998-03-14", "ssn4": "4471",
    }
    candidates = [{
        "idcol_id": "7734",
        "first_name_std": "KATRINA", "last_name_std": "NGUYEN",
        "address1_std": "99 MAPLE DR", "zip5": "02910",  # differs -> rule 10's exact zip5 also fails
        "birth_date": "1998-03-14", "ssn4": "4471",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            assert m.name == "fuzzy_name_addr_combined"
            assert out.decision is MatchDecision.AMBIGUOUS
            assert 0.6 <= out.score < 0.75
            return
    raise AssertionError("expected the fully-fuzzy tier to flag this record for review")
