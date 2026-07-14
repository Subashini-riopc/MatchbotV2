"""SYSTEM$SEND_EMAIL call generation for RUN_MATCH_PIPELINE's run-summary
notification — the Snowflake-native equivalent of the AWS demo's
matchbot.notify.ses.SESNotifier.

Requires a one-time account-level notification integration and per-recipient
email verification, done once outside this code (not managed here, same as
how AWS's SESNotifier assumes the sender/recipient addresses are already
verified in SES):

    CREATE NOTIFICATION INTEGRATION matchbot_email_int
        TYPE = EMAIL
        ENABLED = TRUE
        ALLOWED_RECIPIENTS = ('subashini@adroitts.com', 'nikhil@adroitts.com');

    ALTER USER <username> SET EMAIL = '<address>';   -- if not already set
    CALL SYSTEM$START_USER_EMAIL_VERIFICATION('<username>');  -- per recipient

SYSTEM$SEND_EMAIL can only deliver to addresses belonging to verified
Snowflake users in this account — confirmed live via "Email recipients ...
are not allowed" until both conditions were met for every recipient.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matchbot_snowflake.config_models import MatcherSpec

NOTIFICATION_INTEGRATION = "MATCHBOT_EMAIL_INT"

# Hardcoded, matching the AWS demo's --ses_recipients posture (a fixed,
# comma-separated list, not per-run configurable) — see notify/ses.py.
RECIPIENTS = "subashini@adroitts.com,nikhil@adroitts.com"

# Ported verbatim from matchbot.pipeline.match::_ATTRIBUTE_DISPLAY_NAMES so
# matched_on_attributes() below produces identical labels to the AWS email's
# "Matched on" row for the same matcher chain — kept here rather than
# imported since config_models.py is already a deliberate, hand-synced copy
# of the real matchbot package (see that module's docstring) and this is
# reporting-only logic, same category as the rest of that copy.
_ATTRIBUTE_DISPLAY_NAMES: dict[str, str] = {
    "member_external_id": "External ID",
    "rilds_id": "External ID",
    "first_name": "First Name",
    "first_name_std": "First Name",
    "middle_name": "Middle Name",
    "last_name": "Last Name",
    "last_name_std": "Last Name",
    "birth_date": "Birth Date",
    "ssn": "SSN",
    "gender": "Gender",
}


def _display_name(attribute: str) -> str:
    return _ATTRIBUTE_DISPLAY_NAMES.get(attribute, attribute.replace("_", " ").title())


def matched_on_attributes(matcher_chain: list["MatcherSpec"]) -> list[str]:
    """Human-readable, deduplicated attribute names the chain compares on —
    ports matchbot.pipeline.match::matched_on_attributes()'s exact logic
    (dedup, first-seen order, keys + comparisons) so the Snowflake email's
    "Matched on" line means the same thing as the AWS one: what the
    resolved chain COULD compare on, not what fired for this specific run.
    """
    seen: dict[str, None] = {}
    for spec in matcher_chain:
        for attr in spec.keys:
            seen.setdefault(_display_name(attr), None)
        for comparison in spec.comparisons:
            seen.setdefault(_display_name(comparison.attribute), None)
    return list(seen)


def _escape(value: str) -> str:
    """Escape single quotes for safe interpolation into a SQL string
    literal — the run-summary body only ever contains our own known
    values (file paths, counts, error messages), but error messages in
    particular can contain arbitrary text (e.g. a quoted identifier from
    a SQL compilation error), so this is not optional."""
    return value.replace("'", "''")


def render_success_email_sql(
    file_path: str,
    provider_code: str,
    rows_landed: int,
    rows_rejected: int,
    rows_staged: int,
    rows_matched: int,
    rows_unmatched: int,
    match_rate: float,
    duration_seconds: float,
    run_uid: str,
    matched_on: list[str],
    reference_row_count: int,
    total_columns: int,
    duplicate_row_count: int,
    null_counts: list[tuple[str, int]],
) -> str:
    """CALL SYSTEM$SEND_EMAIL(...) for a successful run.

    null_counts is a list of (column_name, null_count) pairs, in the same
    order as the file's own header — the Snowflake-side equivalent of the
    AWS email's per-column "File profile" table.
    """
    subject = (
        f"MatchBot SUCCESS: {provider_code} "
        f"({rows_matched}/{rows_staged} matched, {match_rate:.1%})"
    )
    total_rows = rows_landed + rows_rejected
    null_lines = "\\n".join(
        f"  {col}: {count} ({(count / rows_landed):.1%})" if rows_landed else f"  {col}: {count}"
        for col, count in null_counts
    )
    body = (
        f"Run: {run_uid}\\n"
        f"File: {file_path}\\n"
        f"Provider: {provider_code}\\n"
        f"Matched on: {', '.join(matched_on) if matched_on else '-'}\\n"
        f"Rows in file: {total_rows}\\n"
        f"Rows rejected: {rows_rejected}\\n"
        f"Rows staged: {rows_staged}\\n"
        f"Rows matched: {rows_matched}\\n"
        f"Rows unmatched: {rows_unmatched}\\n"
        f"Match rate: {match_rate:.1%}\\n"
        f"Reference table rows: {reference_row_count}\\n"
        f"Duration (s): {duration_seconds:.2f}\\n"
        f"\\n"
        f"File profile — as received:\\n"
        f"  Total rows: {rows_landed}\\n"
        f"  Total columns: {total_columns}\\n"
        f"  Duplicate rows: {duplicate_row_count}\\n"
        f"  Null / blank counts by column:\\n"
        f"{null_lines}"
    )
    return (
        f"CALL SYSTEM$SEND_EMAIL("
        f"'{NOTIFICATION_INTEGRATION}', "
        f"'{RECIPIENTS}', "
        f"'{_escape(subject)}', "
        f"'{_escape(body)}')"
    )


def render_failure_email_sql(
    file_path: str,
    error_message: str,
    run_uid: str,
) -> str:
    """CALL SYSTEM$SEND_EMAIL(...) for a failed run."""
    subject = f"MatchBot FAILED: {file_path}"
    body = f"Run: {run_uid}\\nFile: {file_path}\\nError: {error_message}"
    return (
        f"CALL SYSTEM$SEND_EMAIL("
        f"'{NOTIFICATION_INTEGRATION}', "
        f"'{RECIPIENTS}', "
        f"'{_escape(subject)}', "
        f"'{_escape(body)}')"
    )
