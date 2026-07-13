"""Generate a fully synthetic rilds_reference (400k rows) that overlaps with
the dummy RIDE enrollment files from gen_ride_enrollment.py.

No real data anywhere: every identity here comes from gen_ride_enrollment's
person() generator (Faker-driven, random SASIDs) — the SAME function that
script uses to build its own dummy RIDE files. Running both scripts with
their default seeds produces a rilds_reference set whose SASIDs/names
include a deliberate SAMPLE of the people in EACH of gen_ride_enrollment's
1k/10k/100k/1m RIDE files, at each file's own 50-75%-range match rate (see
OVERLAP_RATE below) — enough that a real matching run against this
reference data produces genuine EXACT_SASID hits from any of the four file
sizes, at a realistic (not 100%) match rate. None of it is real PII.

400k rows (not 100k): the 1m RIDE file has ~580k distinct people even
though many of its 1,000,000 rows are repeat enrollments for the same
person (1-4 rows/person) — giving that file a real 50%+ match rate needs
the reference table to hold a comparable number of those distinct people,
which a 100k-row table cannot do regardless of how much row-level
duplication the RIDE file itself has.

This REPLACES the old version of this script, which sourced identities from
a real RIDE enrollment extract (--source path) — that dependency on real
data is exactly what's being removed here.

Run:
    uv run python scripts/gen_rilds_reference.py
"""

from __future__ import annotations

import csv
import random
from pathlib import Path

from faker import Faker

from gen_ride_enrollment import SEED as RIDE_SEED
from gen_ride_enrollment import SIZES as RIDE_SIZES
from gen_ride_enrollment import generate as gen_ride_rows

OUT = Path("data/samples")
SEED = 2026
REFERENCE_SIZE = 400_000

# Independent per-field coverage for fabricated columns RIDE's schema has no
# equivalent for at all (SSN/DOB/gender/address) — mixed/lower coverage to
# simulate realistic reference-data gaps, per the agreed design. gender IS
# on the RIDE side (SEX), but reference gender is generated independently
# here rather than copied 1:1, since rilds_reference is meant to model a
# separately-sourced identity record, not a mirror of one provider's file.
SSN_COVERAGE = 0.50
DOB_COVERAGE = 0.55
GENDER_COVERAGE = 0.60
ADDRESS_COVERAGE = 0.45

RI_CITIES = [
    ("PROVIDENCE", "RI", "02903"),
    ("CRANSTON", "RI", "02920"),
    ("WARWICK", "RI", "02886"),
    ("PAWTUCKET", "RI", "02860"),
    ("WOONSOCKET", "RI", "02895"),
    ("COVENTRY", "RI", "02816"),
    ("BRISTOL", "RI", "02809"),
    ("NEWPORT", "RI", "02840"),
]


def _people_from_ride_generator() -> dict[str, list[dict]]:
    """Reproduce gen_ride_enrollment.py's exact main() sequence (same seed,
    same generate() calls, same order/sizes) and pull distinct SASID/name
    identities straight out of the rows it actually returns — keyed by RIDE
    file size ('1k', '10k', '100k', '1m') so the caller can sample a slice
    from EACH size rather than an arbitrary prefix of a merged list (a
    merged list would be dominated by whichever size iterates first/last —
    the 1m file alone has ~580k unique people, dwarfing the other three
    combined, so a naive people[:REFERENCE_SIZE] slice risks leaving the
    smaller files with little or no overlap depending on dict order).

    Not a re-derivation of its RNG draws (person() alone doesn't consume the
    stream the same way generate() does — generate() also calls
    random.choices() per person for the enrollment-row count, interleaved
    with person()'s own draws) — calling generate() itself is the only way
    to guarantee the identities here are exactly the ones
    gen_ride_enrollment.py's own run will produce, without duplicating and
    risking drift from its internal RNG consumption pattern.
    """
    random.seed(RIDE_SEED)
    fake = Faker()
    Faker.seed(RIDE_SEED)

    people_by_size: dict[str, list[dict]] = {}
    recid = 900_000
    for size_label, n in RIDE_SIZES.items():
        rows = gen_ride_rows(n, fake, recid)
        recid += len(rows) + 1000
        seen_sasids: set[str] = set()
        people: list[dict] = []
        for row in rows:
            first_name, last_name, sasid = row[1], row[3], row[5]
            if sasid in seen_sasids:
                continue
            seen_sasids.add(sasid)
            people.append({"sasid": sasid, "first_name": first_name, "last_name": last_name})
        people_by_size[size_label] = people
    return people_by_size


def _fake_ssn(fake: Faker) -> str:
    return f"{fake.random_int(100_000_000, 999_999_999)}"


def _fake_address(fake: Faker, rng: random.Random) -> tuple[str, str, str]:
    city, state, zip5 = rng.choice(RI_CITIES)
    return fake.street_address().upper(), city, zip5


def build_reference_rows(people: list[dict], std_config, fake: Faker, rng: random.Random) -> list[dict]:
    from matchbot.matching import standardize as S

    rows = []
    for i, p in enumerate(people, start=1):
        first_name = p["first_name"]
        last_name = p["last_name"]
        first_std = S.std_name(first_name, std_config)
        last_std = S.std_name(last_name, std_config)

        has_ssn = rng.random() < SSN_COVERAGE
        has_dob = rng.random() < DOB_COVERAGE
        has_gender = rng.random() < GENDER_COVERAGE
        has_address = rng.random() < ADDRESS_COVERAGE

        ssn = _fake_ssn(fake) if has_ssn else ""
        birth_date = (
            fake.date_of_birth(minimum_age=17, maximum_age=30).isoformat() if has_dob else ""
        )
        gender = rng.choice(["MALE", "FEMALE"]) if has_gender else ""
        if has_address:
            address1, city, zip5 = _fake_address(fake, rng)
        else:
            address1, city, zip5 = "", "", ""

        by, bm, bd = (birth_date.split("-") if birth_date else ("", "", ""))
        full_name_std = f"{first_std or ''}{last_std or ''}"

        rows.append(
            {
                "idcol_id": i,
                "person_id": i,  # 1:1 for this sample: one idcol per person, already linked
                "dataset_id": 1,  # single synthetic "RIDE extract" dataset
                "first_name": first_name,
                "middle_name": "",
                "last_name": last_name,
                "birth_date": birth_date,
                "gender": gender,
                "ssn": ssn,
                # ModelIdentifiers: only sasid is populated; the other 29 are
                # genuinely unknown for this synthetic source and left blank.
                "apprentice_id": "", "brown_id": "", "bryant_id": "", "ccri_id": "",
                "college_board_id": "", "dcyf_id": "", "dlt_ern": "", "employri_id": "",
                "ged_id": "", "jwu_id": "", "kidsnet_child_id": "", "laces_id": "",
                "laces_staff_id": "", "laces_student_id": "", "lasid": "", "nspid": "",
                "ods": "", "ric_id": "", "ride_cert_id": "", "ridoh_lead_id": "",
                "risd_id": "", "rjri_id": "", "rwu_id": "", "salve_id": "",
                "sasid": p["sasid"],
                "uri_id": "", "voter_id": "", "workforce_id": "",
                "providencecollege_id": "", "netech_id": "",
                # DerivedIdentifiers, computed with the pipeline's own
                # standardize.py so they're consistent with cleanse-stage
                # output. Dual-metaphone (metaphone2) isn't computed here —
                # this sample only reproduces the primary code.
                "first_name_std": first_std or "",
                "first_name_metaphone1": S.metaphone(first_std) or "",
                "first_name_metaphone2": "",
                "first_name_transposed": "",
                "first_initial": (first_name[:1] if first_name else ""),
                "middle_name_std": "",
                "middle_initial": "",
                "last_name_std": last_std or "",
                "last_name_metaphone1": S.metaphone(last_std) or "",
                "last_name_metaphone2": "",
                "last_name_transposed": "",
                "last_name_suffix": "",
                "last_initial": (last_name[:1] if last_name else ""),
                "last_name8": (last_std or "")[:8],
                "full_name_std": full_name_std,
                "full_name_metaphone": S.metaphone(full_name_std) or "",
                "full_name_transposed": "",
                "full_name_dob": f"{full_name_std}{birth_date}" if birth_date else "",
                "birth_month": bm,
                "birth_day": bd,
                "birth_year": by,
                "ssn4": (ssn[-4:] if ssn else ""),
                "address_source": ("synthetic" if has_address else ""),
                "address1": address1,
                "address2": "",
                "city": city,
                "state": ("RI" if has_address else ""),
                "zip": zip5,
            }
        )
    return rows


REFERENCE_FIELDNAMES = [
    "idcol_id", "person_id", "dataset_id",
    "first_name", "middle_name", "last_name", "birth_date", "gender", "ssn",
    "apprentice_id", "brown_id", "bryant_id", "ccri_id", "college_board_id",
    "dcyf_id", "dlt_ern", "employri_id", "ged_id", "jwu_id", "kidsnet_child_id",
    "laces_id", "laces_staff_id", "laces_student_id", "lasid", "nspid", "ods",
    "ric_id", "ride_cert_id", "ridoh_lead_id", "risd_id", "rjri_id", "rwu_id",
    "salve_id", "sasid", "uri_id", "voter_id", "workforce_id",
    "providencecollege_id", "netech_id",
    "first_name_std", "first_name_metaphone1", "first_name_metaphone2",
    "first_name_transposed", "first_initial", "middle_name_std", "middle_initial",
    "last_name_std", "last_name_metaphone1", "last_name_metaphone2",
    "last_name_transposed", "last_name_suffix", "last_initial", "last_name8",
    "full_name_std", "full_name_metaphone", "full_name_transposed", "full_name_dob",
    "birth_month", "birth_day", "birth_year", "ssn4",
    "address_source", "address1", "address2", "city", "state", "zip",
]


def write_reference_csv(rows: list[dict]) -> None:
    path = OUT / "rilds_reference_400k.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REFERENCE_FIELDNAMES)
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {path} ({len(rows):,} rows)")



# Per-RIDE-file-size match rate: what PERCENTAGE of THAT size's distinct
# people get a matching row in rilds_reference (not a flat count — a flat
# count produces a different, inconsistent percentage per file size, which
# is confusing to reason about/demo). Percentage is of DISTINCT people
# (unique SASIDs), not raw rows — a person can have 1-4 repeated enrollment
# rows in the RIDE file, but only counts once here.
#
# All four sizes sit in 50-75% of THEIR OWN distinct-people count. This is
# why rilds_reference is 400k rows, not 100k: 55% of the 1m file's ~580k
# distinct people alone is ~320k — a 100k-row table physically cannot hold
# that many, regardless of how much row-level duplication the 1m file has
# (duplication reduces neither the 1m file's unique-people count nor how
# many of THOSE unique people need a reference row to hit a given %).
OVERLAP_RATE = {"1k": 0.70, "10k": 0.65, "100k": 0.60, "1m": 0.55}


def main() -> None:
    from matchbot.config.loader import load_config

    rng = random.Random(SEED)
    fake = Faker()
    Faker.seed(SEED)
    OUT.mkdir(parents=True, exist_ok=True)

    config = load_config(Path("config"))
    std_config = config.global_config.standardization

    print("Reproducing gen_ride_enrollment.py's identities (1k/10k/100k/1m RIDE files) ...")
    people_by_size = _people_from_ride_generator()
    for size_label, size_people in people_by_size.items():
        print(f"  {size_label}: {len(size_people):,} distinct people")

    sample_rng = random.Random(SEED)
    people: list[dict] = []
    seen_sasids: set[str] = set()
    print("Sampling overlap subset from each RIDE file size ...")
    for size_label, rate in OVERLAP_RATE.items():
        pool = people_by_size.get(size_label, [])
        take = round(len(pool) * rate)
        for p in sample_rng.sample(pool, take):
            if p["sasid"] in seen_sasids:
                continue
            seen_sasids.add(p["sasid"])
            people.append(p)
        print(f"  {size_label}: sampled {take:,} of {len(pool):,} ({rate:.1%}) for overlap")

    if len(people) < REFERENCE_SIZE:
        print(f"Padding with {REFERENCE_SIZE - len(people):,} additional independent synthetic people ...")
        pad_fake = Faker()
        Faker.seed(SEED + 1)
        pad_rng = random.Random(SEED + 1)
        while len(people) < REFERENCE_SIZE:
            sasid = str(pad_fake.random_int(2_000_000_000, 2_999_999_999))
            if sasid in seen_sasids:
                continue
            seen_sasids.add(sasid)
            people.append(
                {
                    "sasid": sasid,
                    "first_name": pad_fake.first_name().upper(),
                    "last_name": pad_fake.last_name().upper(),
                }
            )
        rng = pad_rng  # coverage rolls below continue from the padding RNG
    elif len(people) > REFERENCE_SIZE:
        people = sample_rng.sample(people, REFERENCE_SIZE)

    print("Building rilds_reference rows ...")
    rows = build_reference_rows(people[:REFERENCE_SIZE], std_config, fake, rng)
    write_reference_csv(rows)

    print("Done.")


if __name__ == "__main__":
    main()
