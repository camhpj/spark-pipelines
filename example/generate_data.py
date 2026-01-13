"""Generate a dummy patients dataset for local examples."""

import argparse
import csv
import random
from datetime import date
from pathlib import Path

from faker import Faker


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Generate example patient data.")
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "patients.csv",
    )
    return parser.parse_args()


def generate_rows(row_count: int, seed: int) -> list[dict[str, str]]:
    """Generate patient rows.

    Args:
        row_count: Number of rows to generate.
        seed: Seed for deterministic output.

    Returns:
        List of row dictionaries.
    """

    faker = Faker()
    faker.seed_instance(seed)
    random.seed(seed)

    rows: list[dict[str, str]] = []
    for index in range(row_count):
        person_id = f"P{index:05d}"
        dob = faker.date_between(date(1940, 1, 1), date(2010, 12, 31))
        as_of = faker.date_between(date(2023, 1, 1), date(2025, 12, 31))
        rows.append(
            {
                "person_id": person_id,
                "date_of_birth": dob.isoformat(),
                "as_of_date": as_of.isoformat(),
            }
        )
    return rows


def write_csv(rows: list[dict[str, str]], path: Path) -> None:
    """Write rows to CSV.

    Args:
        rows: Row data.
        path: Output path.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["person_id", "date_of_birth", "as_of_date"],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    """Generate the dataset."""

    args = parse_args()
    rows = generate_rows(args.rows, args.seed)
    write_csv(rows, args.out)
    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
