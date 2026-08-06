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


def test_provider_mapping_address1_zip_ssn_keeps_the_derived_rules(app_config: AppConfig) -> None:
    """Regression test for a real bug: _DERIVED_ATTRIBUTE_SOURCE was
    missing address1_std/zip5/ssn4 entries, so a provider mapping the raw
    canonical attributes address1/zip/ssn still had every rule keyed on
    the corresponding derived column (address1_std, zip5, ssn4) silently
    dropped from its resolved chain — confirmed live via a RISOS
    "Matched on" email missing Address/City/State/Zip despite RISOS
    mapping all four. A provider mapping first_name/last_name/address1/
    city/state/zip/ssn must keep deterministic_name_addr_city_state,
    deterministic_name_addr_zip, and deterministic_name_ssn4 in its
    resolved chain, not just deterministic_external_id/name_dob."""
    g = app_config.global_config
    mapped_attributes = {
        "first_name", "last_name", "address1", "city", "state", "zip", "ssn",
    }
    chain = filter_chain_by_provider_attributes(g.matching.matchers, mapped_attributes)
    names = {m.name for m in chain}
    assert "deterministic_name_addr_city_state" in names
    assert "deterministic_name_addr_zip" in names
    assert "deterministic_name_ssn4" in names


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
    # A candidate that matches exactly on the name+addr+city+state tier
    # (rule 5) AND would also score well under the fuzzy tiers below it —
    # the exact rule must win since it's tried first.
    candidates = [{
        "idcol_id": "1",
        "first_name_std": "ROBERT", "last_name_std": "SMITH",
        "address1_std": "123 MAIN ST", "city": "PROVIDENCE", "state": "RI", "zip5": "02901",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            assert m.name == "deterministic_name_addr_city_state"
            assert out.decision is MatchDecision.MATCHED
            assert out.score == 1.0
            return
    raise AssertionError("expected the name+addr+city+state exact tier to match")


def test_fuzzy_exact_name_addr_scores_purely_on_address_when_gates_pass(
    app_config: AppConfig,
) -> None:
    """fuzzy_exact_name_addr scores purely on address1_std (weight 100,
    threshold 0.90) — first_name_std/last_name_std/zip5 carry weight 0
    (don't affect the score) but are all required: true, a real hard
    precondition enforced identically on both platforms. With exact
    first/last name + exact zip (all required fields satisfied) and a
    similar-but-not-identical address, the match succeeds purely on
    address1_std clearing its own threshold."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE", "zip5": "02909",
    }
    candidates = [{
        "idcol_id": "7734",
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVENUE", "zip5": "02909",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            assert m.name == "fuzzy_exact_name_addr"
            assert out.decision is MatchDecision.MATCHED
            assert out.score == 1.0
            return
    raise AssertionError("expected fuzzy_exact_name_addr to match on address1_std alone")


def test_fuzzy_exact_name_addr_blocks_a_mismatched_first_name_even_with_similar_address(
    app_config: AppConfig,
) -> None:
    """Regression test for the real bug required=true fixes: previously
    (weight=0, no required flag) a totally different first name
    (KATRINA vs KATHERINE) could still match here purely on address1_std
    text similarity. Now that first_name_std is required=true, a
    candidate whose first name isn't EXACT is disqualified outright,
    regardless of how similar the address text looks — enforced
    identically on both platforms (Python's _passes_required() and
    Snowflake's join_predicate_sql AND condition)."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE", "zip5": "02909",
    }
    candidates = [{
        "idcol_id": "7734",
        "first_name_std": "KATRINA", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE", "zip5": "02909",
    }]

    from matchbot.matching.fuzzy import FuzzyMatcher

    fuzzy_exact_name_addr = next(
        m for m in matchers if isinstance(m, FuzzyMatcher) and m.name == "fuzzy_exact_name_addr"
    )
    out = fuzzy_exact_name_addr.match(record, candidates)
    assert out.decision is MatchDecision.UNMATCHED


def test_fuzzy_exact_name_addr_blocks_a_mismatched_zip_even_with_identical_address_text(
    app_config: AppConfig,
) -> None:
    """Regression test for the ORIGINAL bug report: two different
    "78 Oak Ave"s in different zip codes (different towns) must NOT
    match here, even with byte-identical address text and exact name —
    zip5 is required=true specifically to close this gap."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE", "zip5": "02909",
    }
    candidates = [{
        "idcol_id": "7734",
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE", "zip5": "02116",  # different town entirely
    }]

    from matchbot.matching.fuzzy import FuzzyMatcher

    fuzzy_exact_name_addr = next(
        m for m in matchers if isinstance(m, FuzzyMatcher) and m.name == "fuzzy_exact_name_addr"
    )
    out = fuzzy_exact_name_addr.match(record, candidates)
    assert out.decision is MatchDecision.UNMATCHED


def test_fuzzy_name_exact_addr_reached_when_address_is_not_similar(
    app_config: AppConfig,
) -> None:
    """fuzzy_exact_name_addr now scores purely on address1_std (weight
    100, threshold 0.90) — a genuinely dissimilar address means it can
    never fire, regardless of how close the names are. A record with
    close-but-not-exact names (each clears fuzzy_name_exact_addr's own
    0.90 per-field threshold) and a completely different address must
    fall through to fuzzy_name_exact_addr (which scores purely on names)
    and match there — proving that rule is still reachable, just second
    in priority now."""
    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)

    record = {
        "first_name_std": "KATHERINE", "last_name_std": "NGUYEN",
        "address1_std": "78 OAK AVE", "zip5": "02909",
    }
    candidates = [{
        "idcol_id": "7734",
        "first_name_std": "KATHERIN", "last_name_std": "NGUYIN",
        "address1_std": "99 COMPLETELY DIFFERENT RD", "zip5": "02909",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        if out.decision in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS):
            assert m.name == "fuzzy_name_exact_addr"
            assert out.decision is MatchDecision.MATCHED
            return
    raise AssertionError("expected fuzzy_name_exact_addr to match this record")


def test_fuzzy_name_exact_addr_no_longer_masks_a_wrong_first_name_with_address(
    app_config: AppConfig,
) -> None:
    """Regression test for the real bug fuzzy_name_exact_addr's rework
    fixes: exact last_name + exact address + exact zip used to total
    255/300 = 0.85, meeting the OLD accept_threshold with ZERO
    contribution from first_name — so a totally different first name
    (e.g. "MICHAEL" vs "ROBERT") could still auto-match on last name +
    address alone via this specific rule. Now that address1_std/zip5
    carry weight 0 on fuzzy_name_exact_addr (anchor-only, not scored), a
    first name that doesn't clear the 0.90 threshold must prevent THIS
    rule specifically from firing, even with an otherwise-perfect last
    name + exact address. (fuzzy_exact_name_addr firing instead, via its
    own separate exact-last-name+fuzzy-address scoring, is expected and
    out of scope for this change.)"""
    from matchbot.matching.fuzzy import FuzzyMatcher

    g = app_config.global_config
    matchers = build_matchers(g.matching.matchers, g.standardization)
    fuzzy_name_exact_addr = next(
        m for m in matchers if isinstance(m, FuzzyMatcher) and m.name == "fuzzy_name_exact_addr"
    )

    record = {
        "first_name_std": "ROBERT", "last_name_std": "SMITH",
        "address1_std": "45 ELM ST", "zip5": "02903",
    }
    candidates = [{
        "idcol_id": "9001",
        "first_name_std": "MICHAEL", "last_name_std": "SMITH",
        "address1_std": "45 ELM ST", "zip5": "02903",
    }]

    out = fuzzy_name_exact_addr.match(record, candidates)
    assert out.decision is MatchDecision.UNMATCHED


def test_no_match_when_only_the_removed_combined_tier_would_have_caught_it(
    app_config: AppConfig,
) -> None:
    """fuzzy_name_addr_combined (rule 3 — nothing held fully exact, six
    weak/partial signals corroborating each other) was removed per
    explicit request. A record with a dissimilar address (jaro_winkler
    ~0.52, clears neither remaining fuzzy rule's 0.85 address threshold)
    and a differing zip5 falls through every exact tier AND both
    remaining fuzzy tiers — this used to reach fuzzy_name_addr_combined
    and land in review; it must now end up genuinely UNMATCHED, since
    that catch-all tier no longer exists."""
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
        "address1_std": "99 MAPLE DR", "zip5": "02910",  # differs -> exact zip5 also fails
        "birth_date": "1998-03-14", "ssn4": "4471",
    }]

    for m in matchers:
        out = m.match(record, candidates)
        assert out.decision not in (MatchDecision.MATCHED, MatchDecision.AMBIGUOUS), (
            f"expected no matcher to fire, but {m.name} did"
        )
