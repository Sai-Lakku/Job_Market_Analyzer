import csv
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "Data" / "synthetic_data.csv"
OUTPUT_FILE = BASE_DIR / "Data" / "clean_data.csv"
REPORT_FILE = BASE_DIR / "Data" / "report_data.txt"

EXPECTED_FIELDS = [
    "record_id",
    "company",
    "job_title",
    "location",
    "salary_usd",
    "years_experience",
    "date_posted",
    "skills",
]
REQUIRED_FIELDS = set(EXPECTED_FIELDS)


def _is_null(value: Optional[str]) -> bool:
    if value is None:
        return True
    return value.strip().lower() in {"", "na", "n/a", "none", "null", "nan"}


def _clean_text(value: str) -> str:
    return " ".join(value.strip().split())


def _clean_skills(raw_value: str) -> str:
    skill_parts = [_clean_text(item) for item in raw_value.split(",")]
    skill_parts = [skill for skill in skill_parts if skill]
    deduped = list(dict.fromkeys(skill_parts))
    return ", ".join(deduped)


def _human_title_case(value: str) -> str:
    return " ".join(part.capitalize() for part in value.split())


def _parse_salary(raw_value: str) -> Optional[Tuple[int, int]]:
    if "-" not in raw_value:
        return None
    low, high = raw_value.split("-", 1)
    low = low.strip()
    high = high.strip()
    if not low.isdigit() or not high.isdigit():
        return None

    min_salary = int(low)
    max_salary = int(high)
    if min_salary <= 0 or max_salary <= 0 or min_salary > max_salary:
        return None
    return min_salary, max_salary


def _clean_and_validate_row(row: Dict[str, str]) -> Tuple[Optional[Dict[str, str]], str]:
    for field in REQUIRED_FIELDS:
        if field not in row or _is_null(row[field]):
            return None, "null_or_missing_required_field"

    cleaned_row = {key: _clean_text(value) for key, value in row.items()}

    if not cleaned_row["record_id"].isdigit():
        return None, "invalid_record_id"

    if not cleaned_row["years_experience"].isdigit():
        return None, "invalid_years_experience"
    years_experience = int(cleaned_row["years_experience"])
    if years_experience < 0 or years_experience > 50:
        return None, "invalid_years_experience"
    cleaned_row["years_experience"] = str(years_experience)

    salary = _parse_salary(cleaned_row["salary_usd"])
    if salary is None:
        return None, "invalid_salary_range"
    cleaned_row["salary_usd"] = f"{salary[0]}-{salary[1]}"

    try:
        posted_date = datetime.strptime(cleaned_row["date_posted"], "%Y-%m-%d")
        cleaned_row["date_posted"] = posted_date.strftime("%Y-%m-%d")
    except ValueError:
        return None, "invalid_date_posted"

    cleaned_row["company"] = _human_title_case(cleaned_row["company"])
    cleaned_row["job_title"] = _human_title_case(cleaned_row["job_title"])
    cleaned_row["location"] = _human_title_case(cleaned_row["location"])
    cleaned_row["skills"] = _clean_skills(cleaned_row["skills"])
    if _is_null(cleaned_row["skills"]):
        return None, "invalid_skills"

    output_row = {field: cleaned_row[field] for field in EXPECTED_FIELDS}
    return output_row, ""


def _row_signature(row: Dict[str, str]) -> Tuple[str, ...]:
    return (
        row["company"],
        row["job_title"],
        row["location"],
        row["salary_usd"],
        row["years_experience"],
        row["date_posted"],
        row["skills"],
    )


def clean_dataset(
    input_file: Path = INPUT_FILE,
    output_file: Path = OUTPUT_FILE,
    report_file: Path = REPORT_FILE,
) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file.resolve()}")

    with input_file.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_rows = list(reader)

    total_rows = len(input_rows)
    issue_counts: Counter[str] = Counter()
    cleaned_rows: List[Dict[str, str]] = []
    seen_ids = set()
    seen_signatures = set()

    for row in input_rows:
        cleaned, issue = _clean_and_validate_row(row)
        if cleaned is None:
            issue_counts[issue] += 1
            continue

        record_id = cleaned["record_id"]
        if record_id in seen_ids:
            issue_counts["duplicate_record_id"] += 1
            continue

        signature = _row_signature(cleaned)
        if signature in seen_signatures:
            issue_counts["duplicate_content_row"] += 1
            continue

        seen_ids.add(record_id)
        seen_signatures.add(signature)
        cleaned_rows.append(cleaned)

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPECTED_FIELDS)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    removed_rows = total_rows - len(cleaned_rows)
    completion_rate = 0.0 if total_rows == 0 else (len(cleaned_rows) / total_rows) * 100

    report_lines = [
        "Synthetic Data Cleaning Report",
        f"Input file: {input_file.resolve()}",
        f"Output file: {output_file.resolve()}",
        "",
        f"Total input rows: {total_rows}",
        f"Rows retained: {len(cleaned_rows)}",
        f"Rows removed: {removed_rows}",
        f"Retention rate: {completion_rate:.2f}%",
        "",
        "Removal reasons:",
    ]

    if issue_counts:
        for reason, count in sorted(issue_counts.items(), key=lambda item: item[0]):
            report_lines.append(f"- {reason}: {count}")
    else:
        report_lines.append("- none")

    report_file.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"Cleaned data written to: {output_file.resolve()}")
    print(f"Cleaning report written to: {report_file.resolve()}")
    print(
        f"Rows: input={total_rows}, cleaned={len(cleaned_rows)}, "
        f"removed={removed_rows}, retention={completion_rate:.2f}%"
    )


if __name__ == "__main__":
    clean_dataset()
