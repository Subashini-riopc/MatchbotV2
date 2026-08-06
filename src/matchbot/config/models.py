"""Pydantic models for the layered YAML configuration.

These models define and validate the *domain* configuration:

* ``global.yaml``  -> :class:`GlobalConfig`  (canonical attrs, standardization,
  blocking, matchers, thresholds, DQ rules — shared by all providers).
* ``providers/*.yaml`` -> :class:`ProviderConfig` (one per provider: file
  format, column mappings, transforms — the only thing onboarding a provider
  requires).

Validation is strict: unknown keys are rejected and cross-references (e.g. a
column mapping pointing at a non-existent canonical attribute) are checked in
:mod:`matchbot.config.loader`, so a bad config fails fast with a clear message
before any data is touched.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from matchbot.domain.enums import FileFormat


class _Strict(BaseModel):
    """Base for all config models: forbid unknown keys to catch typos early."""

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# Transforms (per-attribute cleansing instructions)
# ---------------------------------------------------------------------------
class TransformSpec(_Strict):
    """How to coerce/normalize a single mapped value during cleanse.

    All fields optional; only what a given column needs is specified.
    """

    type: str | None = Field(
        default=None, description="Target logical type: string | date | integer."
    )
    format: str | None = Field(
        default=None, description="strptime format for date parsing, e.g. %m/%d/%Y."
    )
    zero_pad: int | None = Field(
        default=None, ge=1, description="Left-pad to this width (e.g. SSN -> 9)."
    )
    strip: list[str] = Field(
        default_factory=list, description="Substrings to remove (e.g. ['-', ' '])."
    )
    upper: bool = Field(default=False, description="Uppercase the value.")
    trim: bool = Field(default=True, description="Strip surrounding whitespace.")


# ---------------------------------------------------------------------------
# Combined columns (multiple raw source columns -> one canonical attribute)
# ---------------------------------------------------------------------------
class CombinedColumnSpec(_Strict):
    """Concatenate several raw source columns into one canonical attribute.

    column_mappings is strictly 1 raw column -> 1 canonical attribute; some
    providers ship an address (or other field) pre-split across multiple raw
    columns instead (e.g. RISOS's STREET_NUMBER/STREET_NAME, where address1
    must be the full street address for every address-anchored matcher to
    mean anything — a bare house number is a near-useless identity anchor,
    since many unrelated people share one). This is the generic mechanism
    for that: any provider can declare "concatenate these N raw columns, in
    this order, with this separator" for any canonical attribute, not just
    address1 — no per-provider code required.
    """

    from_columns: list[str] = Field(
        min_length=1,
        description="Raw source column names, concatenated in this order.",
    )
    separator: str = Field(default=" ", description="Inserted between each column's value.")


# ---------------------------------------------------------------------------
# Provider config (one file per provider)
# ---------------------------------------------------------------------------
class FixedWidthColumn(_Strict):
    """Column slice for fixed-width files."""

    name: str
    start: int = Field(ge=0)
    length: int = Field(ge=1)


class ProviderConfig(_Strict):
    """Everything needed to ingest one provider — the full onboarding surface."""

    provider_id: str = Field(description="Stable unique id, e.g. 'dlt_ui'.")
    display_name: str
    format: FileFormat
    file_glob: str = Field(description="Glob to select this provider's files.")

    # The S3 folder segment this file type's files land under, e.g.
    # 'risos_voter'. Defaults to provider_id (true for every single-file-type
    # provider so far — RIDE's folder is literally 'ride_enrollment', its
    # provider_id). A multi_file provider's SECOND+ file type must set this
    # explicitly to the shared folder, since its own provider_id
    # (risos_voterhistory) is NOT the folder files actually land in
    # (risos_voter) — see config_bridge.py's provider_folder_name().
    s3_folder: str | None = Field(
        default=None,
        description="S3 folder this file type's files land under; "
        "defaults to provider_id. Set explicitly when it differs (e.g. a "
        "second file type sharing a multi_file provider's folder).",
    )

    # Short agency/provider code and dataset name used in the DB (rilds_stage,
    # rilds_audit, land table name). Default to the provider_id halves so
    # existing providers need no change.
    provider_code: str = Field(
        default="",
        max_length=20,
        description="Short agency code, e.g. 'ride'. Defaults from provider_id.",
    )
    dataset_name: str = Field(
        default="",
        max_length=100,
        description="Dataset name, e.g. 'enrollment'. Defaults from provider_id.",
    )

    # True when this provider ships more than one structurally distinct file
    # under the same folder (e.g. RISOS sends voter registration AND voter
    # history separately, with unrelated columns). False (default) keeps a
    # provider's land table name exactly {provider_code}_land as before —
    # true switches to one table per file type, {provider_code}_{file_type}_land,
    # since a single shared table can't hold two incompatible file shapes
    # (CREATE TABLE IF NOT EXISTS silently no-ops against an existing
    # differently-shaped table, so the second file type's load then fails on
    # missing columns).
    multi_file: bool = Field(
        default=False,
        description="True if this provider sends multiple distinct file "
        "shapes under one folder — land table becomes "
        "{provider_code}_{file_type}_land instead of {provider_code}_land.",
    )

    # False for a file type that goes through land + transform but never
    # person-linkage matching (e.g. RISOS VoterHistory: legacy's
    # PeripheralDataRow — run_linkage=False — never runs First/Second Pass;
    # it rides on the primary Voter file's already-established
    # voter_id -> person_id linkage instead of re-matching itself). True
    # (default) is every provider/file type built so far (RIDE, RISOS
    # Voter) — both go through the full cleanse -> canonical -> match
    # pipeline. run_pipeline.py routes a matches_dataset=False config
    # through land + its own transform step and stops there, never touching
    # RILDS_STAGE/matching at all.
    matches_dataset: bool = Field(
        default=True,
        description="False if this file type only lands + transforms and "
        "never goes through person-linkage matching (e.g. a peripheral/"
        "history file tied to another file's identity resolution).",
    )

    # file column name -> canonical attribute name. Not required for a
    # matches_dataset=False config that instead defines its own custom
    # projection (e.g. VoterHistory's per-election unpivot has no 1:1
    # column mapping to speak of).
    column_mappings: dict[str, str] = Field(
        default_factory=dict,
        description="Maps raw file columns onto canonical attributes.",
    )
    # Canonical attribute -> how to build it from MULTIPLE raw columns (see
    # CombinedColumnSpec). Only for a canonical attribute that a provider's
    # file splits across more than one raw column — column_mappings remains
    # the only mechanism for every ordinary 1:1 raw->canonical mapping.
    combined_columns: dict[str, CombinedColumnSpec] = Field(
        default_factory=dict,
        description="Canonical attribute -> raw columns to concatenate "
        "into it, for a provider whose file splits one canonical field "
        "across multiple raw columns (e.g. street number + street name).",
    )
    # Which rilds_reference column this provider's member_external_id (stored
    # generically as stage.rilds_id) should be compared against for the
    # deterministic_external_id matcher — e.g. 'sasid' for RIDE, 'ccri_id' for
    # a future CCRI provider. Only meaningful when the provider maps
    # member_external_id; omit otherwise.
    external_id_column: str | None = Field(
        default=None,
        description="rilds_reference column that this provider's "
        "member_external_id matches against, e.g. 'sasid'.",
    )
    # canonical attribute -> how to cleanse it
    transforms: dict[str, TransformSpec] = Field(default_factory=dict)
    # rows missing any of these (after mapping) are skipped and counted
    skip_if_null: list[str] = Field(default_factory=list)

    # format-specific options
    delimiter: str = Field(default=",", description="CSV delimiter.")
    has_header: bool = Field(default=True, description="CSV/XLSX header row present.")
    sheet_name: str | int | None = Field(
        default=None, description="XLSX sheet (name or index); None = first sheet."
    )
    fixed_width_columns: list[FixedWidthColumn] = Field(
        default_factory=list, description="Required when format = fixed_width."
    )
    # optional per-provider override of the matcher chain (else global default)
    # Each entry is either a string (reference to a global matcher by name)
    # or an inline MatcherSpec (provider-local definition, overrides global
    # matcher of the same name if one exists).
    matchers: list[str | MatcherSpec] | None = Field(default=None)

    @model_validator(mode="after")
    def _check_format_requirements(self) -> ProviderConfig:
        if self.format is FileFormat.FIXED_WIDTH and not self.fixed_width_columns:
            raise ValueError(
                f"provider {self.provider_id!r}: format=fixed_width requires fixed_width_columns"
            )
        # Derive provider_code / dataset_name from provider_id when omitted.
        # 'ride_enrollment' -> code='ride', dataset='enrollment'.
        if not self.provider_code or not self.dataset_name:
            head, _, tail = self.provider_id.partition("_")
            object.__setattr__(self, "provider_code", self.provider_code or head[:20])
            object.__setattr__(
                self, "dataset_name", self.dataset_name or (tail or head)[:100]
            )
        return self


# ---------------------------------------------------------------------------
# Global config (single shared file)
# ---------------------------------------------------------------------------
class CanonicalAttributeConfig(_Strict):
    """Declares a canonical attribute in config (validated against the domain)."""

    name: str
    dtype: str = Field(description="string | date | integer")
    pii: bool = False


class StandardizationConfig(_Strict):
    """Replaces the legacy hardcoded gender map / suffix list with config."""

    gender_map: dict[str, str] = Field(default_factory=dict)
    name_suffixes: list[str] = Field(default_factory=list)
    name_prefixes: list[str] = Field(default_factory=list)
    address_abbreviations: dict[str, str] = Field(default_factory=dict)


class BlockingKey(_Strict):
    """A blocking key: a tuple of canonical attrs (optionally phonetic) used to
    cheaply narrow candidate members before scoring."""

    name: str
    attributes: list[str] = Field(min_length=1)
    phonetic: list[str] = Field(
        default_factory=list,
        description="Subset of `attributes` to encode phonetically (metaphone).",
    )


class FieldComparison(_Strict):
    """How a single attribute contributes to a fuzzy score."""

    attribute: str
    method: str = Field(
        default="exact",
        description="exact | levenshtein | jaro_winkler | metaphone",
    )
    weight: float = Field(default=1.0, ge=0.0)
    threshold: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Per-field similarity (0-1) at/above which the field agrees.",
    )
    # A required comparison must clear its own threshold or the WHOLE
    # matcher returns no-match for that candidate immediately — enforced
    # BEFORE scoring, identically on both platforms (unlike weight, which
    # only affects the score and is NOT a real requirement on its own: a
    # weight=0 comparison still contributes to the score, just with zero
    # effect, but a candidate that fails it is scored anyway and can still
    # match via other fields). Added specifically because weight=0 was
    # being used to mean "not required" when in fact it silently permitted
    # exactly that — e.g. a candidate in the wrong zip code/city/state
    # still matching purely on address1_std text similarity. required
    # closes that gap for real, on both matching/fuzzy.py (Python) and
    # matchers/fuzzy.py (Snowflake SQL).
    required: bool = Field(
        default=False,
        description="If true, this comparison must clear its threshold or "
        "the matcher never fires for this candidate, regardless of the "
        "overall weighted score.",
    )


class MatcherSpec(_Strict):
    """A named matcher in the chain (deterministic or fuzzy)."""

    name: str
    type: str = Field(description="deterministic | fuzzy")
    enabled: bool = True
    # deterministic: list of canonical attrs that must all match exactly
    keys: list[str] = Field(default_factory=list)
    # fuzzy: weighted field comparisons + accept threshold
    comparisons: list[FieldComparison] = Field(default_factory=list)
    accept_threshold: float = Field(default=0.85, ge=0.0, le=1.0)
    review_threshold: float = Field(
        default=0.60,
        ge=0.0,
        le=1.0,
        description="Below accept but >= this -> AMBIGUOUS (review), not unmatched.",
    )


class MatchConfig(_Strict):
    """The matching configuration: blocking + ordered matcher chain."""

    blocking_keys: list[BlockingKey] = Field(default_factory=list)
    matchers: list[MatcherSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_matcher_names(self) -> MatchConfig:
        names = [m.name for m in self.matchers]
        if len(names) != len(set(names)):
            raise ValueError("matcher names must be unique")
        return self


class DQRule(_Strict):
    """A single data-quality rule evaluated during cleanse, recorded as metric."""

    name: str
    attribute: str
    rule: str = Field(description="not_null | regex | in_set | length")
    pattern: str | None = None
    allowed: list[str] = Field(default_factory=list)
    min_length: int | None = None
    max_length: int | None = None
    severity: str = Field(default="warn", description="warn | error")


class GlobalConfig(_Strict):
    """The shared, provider-independent configuration."""

    canonical_attributes: list[CanonicalAttributeConfig]
    standardization: StandardizationConfig = Field(default_factory=StandardizationConfig)
    matching: MatchConfig
    dq_rules: list[DQRule] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# The fully-assembled application config
# ---------------------------------------------------------------------------
class AppConfig(_Strict):
    """Global config plus all providers, assembled and cross-validated."""

    global_config: GlobalConfig
    providers: dict[str, ProviderConfig]

    def provider(self, provider_id: str) -> ProviderConfig:
        try:
            return self.providers[provider_id]
        except KeyError:
            known = ", ".join(sorted(self.providers)) or "<none>"
            raise KeyError(f"Unknown provider {provider_id!r}. Known providers: {known}") from None
