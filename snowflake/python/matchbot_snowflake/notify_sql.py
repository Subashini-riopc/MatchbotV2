"""SYSTEM$SEND_EMAIL call generation for RUN_MATCH_PIPELINE's run-summary
notification — the Snowflake-native equivalent of the AWS demo's
matchbot.notify.ses.SESNotifier. Sent as an HTML table body (mime_type
'text/html'), matching the layout SESNotifier already builds on the AWS
side (see notify/ses.py's _build_html), rather than a flat text dump.

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

_TD = 'style="padding:6px 12px;border:1px solid #ddd;"'
_TD_LABEL = 'style="padding:6px 12px;border:1px solid #ddd;font-weight:bold;"'
_TH = 'style="padding:6px 12px;border:1px solid #ddd;background:#f2f2f2;text-align:left;"'


def _html_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _row_html(label: str, value: str) -> str:
    return f"<tr><td {_TD_LABEL}>{_html_escape(label)}</td><td {_TD}>{_html_escape(value)}</td></tr>"

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
    "ssn4": "SSN",
    "gender": "Gender",
    "address1": "Address",
    "address1_std": "Address",
    "city": "City",
    "state": "State",
    "zip": "Zip",
    "zip5": "Zip",
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


def _file_profile_html(
    total_columns: int,
    duplicate_row_count: int,
    rows_landed: int,
    null_counts: list[tuple[str, int]],
) -> str:
    """One row per source column's null/blank count, plus the summary
    counts above it — HTML equivalent of the AWS email's file-profile
    table (see notify/ses.py's _file_profile_html), built from the same
    inputs the old plain-text version used."""
    if not null_counts:
        return ""
    header = "<tr>" + "".join(f"<th {_TH}>{h}</th>" for h in ("Column", "Null / Blank Count", "Null / Blank %")) + "</tr>"
    body_rows = []
    for col, count in null_counts:
        pct = (count / rows_landed) if rows_landed else 0.0
        body_rows.append(
            f"<tr><td {_TD}>{_html_escape(col)}</td><td {_TD}>{count}</td><td {_TD}>{pct:.1%}</td></tr>"
        )
    return f"""\
    <h3 style="margin-bottom:4px;">File profile — as received</h3>
    <table style="border-collapse:collapse;margin-bottom:8px;">
      {_row_html("Total rows", str(rows_landed))}
      {_row_html("Total columns", str(total_columns))}
      {_row_html("Duplicate rows", str(duplicate_row_count))}
    </table>
    <table style="border-collapse:collapse;margin-bottom:16px;">
      {header}
      {"".join(body_rows)}
    </table>"""


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
    """CALL SYSTEM$SEND_EMAIL(...) for a successful run, HTML body.

    null_counts is a list of (column_name, null_count) pairs, in the same
    order as the file's own header — feeds the "File profile" table.
    """
    subject = (
        f"MatchBot SUCCESS: {provider_code} "
        f"({rows_matched}/{rows_staged} matched, {match_rate:.1%})"
    )
    total_rows = rows_landed + rows_rejected
    summary_rows = [
        _row_html("Run", run_uid),
        _row_html("File", file_path),
        _row_html("Provider", provider_code),
        _row_html("Matched on", ", ".join(matched_on) if matched_on else "-"),
        _row_html("Rows in file", str(total_rows)),
        _row_html("Rows rejected", str(rows_rejected)),
        _row_html("Rows staged", str(rows_staged)),
        _row_html("Rows matched", str(rows_matched)),
        _row_html("Rows unmatched", str(rows_unmatched)),
        _row_html("Match rate", f"{match_rate:.1%}"),
        _row_html("Reference table rows", str(reference_row_count)),
        _row_html("Duration (s)", f"{duration_seconds:.2f}"),
    ]
    body = f"""\
<html>
  <body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222;">
    <h2 style="margin-bottom:4px;">MatchBot run summary</h2>
    <table style="border-collapse:collapse;margin-bottom:16px;">
      {"".join(summary_rows)}
    </table>
    {_file_profile_html(total_columns, duplicate_row_count, rows_landed, null_counts)}
  </body>
</html>"""
    return (
        f"CALL SYSTEM$SEND_EMAIL("
        f"'{NOTIFICATION_INTEGRATION}', "
        f"'{RECIPIENTS}', "
        f"'{_escape(subject)}', "
        f"'{_escape(body)}', "
        f"'text/html')"
    )


def render_failure_email_sql(
    file_path: str,
    error_message: str,
    run_uid: str,
) -> str:
    """CALL SYSTEM$SEND_EMAIL(...) for a failed run, HTML body."""
    subject = f"MatchBot FAILED: {file_path}"
    rows = [
        _row_html("Run", run_uid),
        _row_html("File", file_path),
        _row_html("Error", error_message),
    ]
    body = f"""\
<html>
  <body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222;">
    <h2 style="margin-bottom:4px;color:#b00020;">MatchBot run FAILED</h2>
    <table style="border-collapse:collapse;margin-bottom:16px;">
      {"".join(rows)}
    </table>
  </body>
</html>"""
    return (
        f"CALL SYSTEM$SEND_EMAIL("
        f"'{NOTIFICATION_INTEGRATION}', "
        f"'{RECIPIENTS}', "
        f"'{_escape(subject)}', "
        f"'{_escape(body)}', "
        f"'text/html')"
    )
