"""Self-contained copy of matchbot's config Pydantic models + domain
constants, for use inside the Snowflake stored procedure.

Why a copy instead of importing matchbot.config directly: matchbot's own
config/__init__.py eagerly imports matchbot.config.loader at package-init
time (Python always runs a package's __init__.py before any of its
submodules — unavoidable, confirmed via a live deployment failure), and
loader.py itself imports matchbot.matching.derive, which imports polars.
polars has compiled shared-library components Snowflake's Anaconda channel
flags as unsupported without --allow-shared-libraries — a heavy, unwanted
dependency for a stored procedure that never uses polars at all (matching
here is pure SQL, not Polars DataFrames).

This file is a deliberate, scoped duplication — not a refactor of the core
matchbot package (which is shared with the AWS/Glue/ECS demo and must stay
untouched). Keep this in sync BY HAND with:
    src/matchbot/config/models.py       (Pydantic models)
    src/matchbot/config/loader.py       (load_config + cross-validation)
    src/matchbot/domain/canonical.py    (CANONICAL_NAMES)
    src/matchbot/domain/enums.py        (FileFormat)
    src/matchbot/matching/derive.py     (DERIVED_COLUMNS — just the tuple,
                                          not add_derived_columns() itself,
                                          which is the actual polars user)
If config/global.yaml's schema changes (new field, new matcher type, new
canonical attribute), this file needs the matching update — there is no
automatic guard against drift here, unlike the rest of this package's
reuse of matchbot.config via a shared import. Kept deliberately small
(models + loader only) to keep that manual-sync surface as narrow as
possible.
"""

from __future__ import annotations

from enum import StrEnum
from importlib import resources
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class FileFormat(StrEnum):
    """Supported provider file formats. Mirrors domain/enums.py::FileFormat."""

    CSV = "csv"
    XLSX = "xlsx"
    FIXED_WIDTH = "fixed_width"


# Mirrors domain/canonical.py — the canonical attribute name list only
# (not the full CanonicalAttribute dataclass/description/pii metadata,
# which loader.py's cross-validation doesn't need).
CANONICAL_NAMES: frozenset[str] = frozenset(
    {
        "first_name",
        "middle_name",
        "last_name",
        "birth_date",
        "ssn",
        "gender",
        "address1",
        "address2",
        "city",
        "state",
        "zip",
        "member_external_id",
    }
)

# Mirrors matching/derive.py::DERIVED_COLUMNS exactly — just the column
# name tuple, not add_derived_columns() (the actual polars-dependent code).
DERIVED_COLUMNS: tuple[str, ...] = (
    "first_name_std",
    "last_name_std",
    "first_name_metaphone1",
    "last_name_metaphone1",
    "last_name8",
    "birth_year",
    "birth_month",
    "birth_day",
)

# Mirrors config/loader.py's _STAGE_ONLY_ATTRS + _MATCHER_VALID_ATTRS.
_STAGE_ONLY_ATTRS = frozenset({"rilds_id"})
_MATCHER_VALID_ATTRS = CANONICAL_NAMES | frozenset(DERIVED_COLUMNS) | _STAGE_ONLY_ATTRS


class _Strict(BaseModel):
    """Base for all config models: forbid unknown keys to catch typos early."""

    model_config = ConfigDict(extra="forbid")


class TransformSpec(_Strict):
    type: str | None = None
    format: str | None = None
    zero_pad: int | None = Field(default=None, ge=1)
    strip: list[str] = Field(default_factory=list)
    upper: bool = False
    trim: bool = True


class FixedWidthColumn(_Strict):
    name: str
    start: int = Field(ge=0)
    length: int = Field(ge=1)


class FieldComparison(_Strict):
    attribute: str
    method: str = "exact"
    weight: float = Field(default=1.0, ge=0.0)
    threshold: float = Field(default=1.0, ge=0.0, le=1.0)


class MatcherSpec(_Strict):
    name: str
    type: str
    enabled: bool = True
    keys: list[str] = Field(default_factory=list)
    comparisons: list[FieldComparison] = Field(default_factory=list)
    accept_threshold: float = Field(default=0.85, ge=0.0, le=1.0)
    review_threshold: float = Field(default=0.60, ge=0.0, le=1.0)


class ProviderConfig(_Strict):
    provider_id: str
    display_name: str
    format: FileFormat
    file_glob: str
    provider_code: str = Field(default="", max_length=20)
    dataset_name: str = Field(default="", max_length=100)
    column_mappings: dict[str, str]
    external_id_column: str | None = None
    transforms: dict[str, TransformSpec] = Field(default_factory=dict)
    skip_if_null: list[str] = Field(default_factory=list)
    delimiter: str = ","
    has_header: bool = True
    sheet_name: str | int | None = None
    fixed_width_columns: list[FixedWidthColumn] = Field(default_factory=list)
    matchers: list[str | MatcherSpec] | None = None

    @model_validator(mode="after")
    def _check_format_requirements(self) -> "ProviderConfig":
        if self.format is FileFormat.FIXED_WIDTH and not self.fixed_width_columns:
            raise ValueError(
                f"provider {self.provider_id!r}: format=fixed_width requires fixed_width_columns"
            )
        if not self.provider_code or not self.dataset_name:
            head, _, tail = self.provider_id.partition("_")
            object.__setattr__(self, "provider_code", self.provider_code or head[:20])
            object.__setattr__(
                self, "dataset_name", self.dataset_name or (tail or head)[:100]
            )
        return self


class CanonicalAttributeConfig(_Strict):
    name: str
    dtype: str
    pii: bool = False


class StandardizationConfig(_Strict):
    gender_map: dict[str, str] = Field(default_factory=dict)
    name_suffixes: list[str] = Field(default_factory=list)
    name_prefixes: list[str] = Field(default_factory=list)


class BlockingKey(_Strict):
    name: str
    attributes: list[str] = Field(min_length=1)
    phonetic: list[str] = Field(default_factory=list)


class MatchConfig(_Strict):
    blocking_keys: list[BlockingKey] = Field(default_factory=list)
    matchers: list[MatcherSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_matcher_names(self) -> "MatchConfig":
        names = [m.name for m in self.matchers]
        if len(names) != len(set(names)):
            raise ValueError("matcher names must be unique")
        return self


class DQRule(_Strict):
    name: str
    attribute: str
    rule: str
    pattern: str | None = None
    allowed: list[str] = Field(default_factory=list)
    min_length: int | None = None
    max_length: int | None = None
    severity: str = "warn"


class GlobalConfig(_Strict):
    canonical_attributes: list[CanonicalAttributeConfig]
    standardization: StandardizationConfig = Field(default_factory=StandardizationConfig)
    matching: MatchConfig
    dq_rules: list[DQRule] = Field(default_factory=list)


class AppConfig(_Strict):
    global_config: GlobalConfig
    providers: dict[str, ProviderConfig]

    def provider(self, provider_id: str) -> ProviderConfig:
        try:
            return self.providers[provider_id]
        except KeyError:
            known = ", ".join(sorted(self.providers)) or "<none>"
            raise KeyError(f"Unknown provider {provider_id!r}. Known providers: {known}") from None


class ConfigError(Exception):
    """Raised when configuration is missing, malformed, or inconsistent."""


def _parse_yaml_text(text: str, source_name: str) -> dict[str, Any]:
    """Parse one YAML document already read into memory as ``text``.

    ``source_name`` is only used for error messages — this function never
    touches a filesystem path itself, so it works identically whether the
    text came from a real file (local dev/tests) or importlib.resources
    (the Snowflake stored procedure — see load_config_from_package below
    and its docstring for why plain Path-based file I/O doesn't work
    there: Snowflake's sandbox runs code directly from an un-extracted
    zip via zipimport, so Path.exists()/.read_text() can't see files
    bundled inside that same zip even though `import` can).
    """
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {source_name}: {exc}") from exc
    if data is None:
        raise ConfigError(f"Config file is empty: {source_name}")
    if not isinstance(data, dict):
        raise ConfigError(f"Config file must be a mapping at top level: {source_name}")
    return data


def _build_global_config(global_yaml_text: str) -> GlobalConfig:
    raw = _parse_yaml_text(global_yaml_text, "global.yaml")
    try:
        return GlobalConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Invalid global config (global.yaml):\n{exc}") from exc


def _build_providers(
    provider_yaml_texts: dict[str, str],
) -> dict[str, ProviderConfig]:
    """``provider_yaml_texts`` maps filename (e.g.
    'provider_ride_enrollment.yaml') -> that file's raw YAML text."""
    if not provider_yaml_texts:
        raise ConfigError("No provider YAML files found")

    providers: dict[str, ProviderConfig] = {}
    for filename in sorted(provider_yaml_texts):
        raw = _parse_yaml_text(provider_yaml_texts[filename], filename)
        try:
            provider = ProviderConfig.model_validate(raw)
        except ValidationError as exc:
            raise ConfigError(f"Invalid provider config ({filename}):\n{exc}") from exc
        if provider.provider_id in providers:
            raise ConfigError(
                f"Duplicate provider_id {provider.provider_id!r} (seen again in {filename})"
            )
        providers[provider.provider_id] = provider
    return providers


def _validate_canonical_alignment(gconf: GlobalConfig) -> None:
    config_names = {a.name for a in gconf.canonical_attributes}
    missing = CANONICAL_NAMES - config_names
    extra = config_names - CANONICAL_NAMES
    problems = []
    if missing:
        problems.append(f"missing from config: {sorted(missing)}")
    if extra:
        problems.append(f"unknown to domain: {sorted(extra)}")
    if problems:
        raise ConfigError(
            "Canonical attributes in global.yaml disagree with the domain "
            "module (matchbot.domain.canonical): " + "; ".join(problems)
        )


def _validate_matcher_spec(spec: MatcherSpec, context: str, errors: list[str]) -> None:
    for k in spec.keys:
        if k not in _MATCHER_VALID_ATTRS:
            errors.append(f"{context} matcher {spec.name!r}: unknown key {k!r}")
    for c in spec.comparisons:
        if c.attribute not in _MATCHER_VALID_ATTRS:
            errors.append(
                f"{context} matcher {spec.name!r}: unknown comparison attribute {c.attribute!r}"
            )


def _validate_cross_references(app: AppConfig) -> None:
    errors: list[str] = []

    g = app.global_config
    for bk in g.matching.blocking_keys:
        for attr in bk.attributes:
            if attr not in _MATCHER_VALID_ATTRS:
                errors.append(f"blocking_key {bk.name!r}: unknown attribute {attr!r}")
    for m in g.matching.matchers:
        _validate_matcher_spec(m, "global", errors)
    for rule in g.dq_rules:
        if rule.attribute not in CANONICAL_NAMES:
            errors.append(f"dq_rule {rule.name!r}: unknown attribute {rule.attribute!r}")

    for pid, prov in app.providers.items():
        for col, attr in prov.column_mappings.items():
            if attr not in CANONICAL_NAMES:
                errors.append(
                    f"provider {pid!r}: column {col!r} maps to unknown attribute {attr!r}"
                )
        for attr in prov.transforms:
            if attr not in CANONICAL_NAMES:
                errors.append(f"provider {pid!r}: transform for unknown attribute {attr!r}")
        for attr in prov.skip_if_null:
            if attr not in CANONICAL_NAMES:
                errors.append(f"provider {pid!r}: skip_if_null unknown attribute {attr!r}")
        if prov.matchers:
            known_global = {m.name for m in g.matching.matchers}
            for entry in prov.matchers:
                if isinstance(entry, str):
                    if entry not in known_global:
                        errors.append(
                            f"provider {pid!r}: references unknown global matcher {entry!r}"
                        )
                else:
                    _validate_matcher_spec(entry, f"provider {pid!r}", errors)

    if errors:
        raise ConfigError("Configuration cross-reference errors:\n  - " + "\n  - ".join(errors))


def _build_app_config(global_yaml_text: str, provider_yaml_texts: dict[str, str]) -> AppConfig:
    """I/O-agnostic core: build+validate an AppConfig from raw YAML text
    already read into memory. Both load_config() (filesystem, used by
    local tests) and load_bundled_config() (importlib.resources, used by
    the deployed Snowflake procedure) gather that text differently and
    then delegate here — so the validation logic only exists once.
    """
    gconf = _build_global_config(global_yaml_text)
    _validate_canonical_alignment(gconf)
    providers = _build_providers(provider_yaml_texts)
    app = AppConfig(global_config=gconf, providers=providers)
    _validate_cross_references(app)
    return app


def load_config(config_dir: str | Path) -> AppConfig:
    """Load, validate, and cross-check the full application config from a
    real filesystem directory (local dev / tests — see load_bundled_config
    for the Snowflake-procedure equivalent).

    Behaviorally identical to matchbot.config.loader.load_config — see
    module docstring for why this is a separate copy rather than an import.
    """
    config_dir = Path(config_dir)

    global_path = config_dir / "global.yaml"
    if not global_path.exists():
        raise ConfigError(f"Config file not found: {global_path}")
    global_yaml_text = global_path.read_text(encoding="utf-8")

    providers_dir = config_dir / "providers"
    if not providers_dir.is_dir():
        raise ConfigError(f"Providers directory not found: {providers_dir}")
    provider_paths = sorted(providers_dir.glob("*.yaml")) + sorted(providers_dir.glob("*.yml"))
    if not provider_paths:
        raise ConfigError(f"No provider YAML files found in {providers_dir}")
    provider_yaml_texts = {p.name: p.read_text(encoding="utf-8") for p in provider_paths}

    return _build_app_config(global_yaml_text, provider_yaml_texts)


def load_bundled_config() -> AppConfig:
    """Load, validate, and cross-check the application config bundled
    inside this package (matchbot_snowflake/config/), for use by the
    deployed Snowflake stored procedure.

    Uses importlib.resources rather than plain Path-based file I/O
    because Snowflake's Python sandbox runs stored-procedure code
    directly out of the uploaded zip via zipimport, without extracting it
    to a real filesystem location first. `import matchbot_snowflake...`
    works fine in that mode (zipimport supports finding modules/packages
    inside a zip), but Path("...").exists() / .read_text() do not — they
    need a real file on disk, which zipimport never creates. Confirmed
    live: the deployed zip was shown (via `unzip -l`) to contain
    matchbot_snowflake/config/global.yaml at exactly the path
    Path(__file__)-relative resolution computed, yet .exists() on that
    same path still returned False at runtime.
    importlib.resources is the standard-library mechanism designed to
    read package data files correctly in both cases (on-disk or zipped).
    """
    package_root = resources.files("matchbot_snowflake") / "config"

    global_yaml_text = (package_root / "global.yaml").read_text(encoding="utf-8")

    providers_dir = package_root / "providers"
    provider_yaml_texts = {
        entry.name: entry.read_text(encoding="utf-8")
        for entry in providers_dir.iterdir()
        if entry.name.endswith((".yaml", ".yml"))
    }
    if not provider_yaml_texts:
        raise ConfigError("No provider YAML files found in bundled config/providers/")

    return _build_app_config(global_yaml_text, provider_yaml_texts)
