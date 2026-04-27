import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_FILE = Path("synthetic_job_data.csv")
RECORDS_TO_ADD = 8000
ERROR_RATE = 0.22
NULL_RATE = 0.10
CONTENT_DUPLICATE_RATE = 0.05
RECORD_ID_DUPLICATE_RATE = 0.03

COMPANIES = [
    "Acme Analytics",
    "Pioneer Data Systems",
    "Nimbus Labs",
    "Vertex Insight",
    "BluePeak Software",
    "Summit AI Works",
    "Aurora Metrics",
    "Cobalt Intelligence",
]

TITLES = [
    "Data Analyst",
    "Business Analyst",
    "Data Engineer",
    "Machine Learning Engineer",
    "Analytics Engineer",
    "Product Analyst",
    "BI Developer",
    "Research Scientist",
]

LOCATIONS = [
    "New York, NY",
    "Austin, TX",
    "Chicago, IL",
    "San Francisco, CA",
    "Seattle, WA",
    "Boston, MA",
    "Remote",
]

SKILLS = [
    "Python",
    "SQL",
    "Tableau",
    "Power BI",
    "AWS",
    "Spark",
    "Pandas",
    "TensorFlow",
    "Airflow",
    "dbt",
]

FIELDS = [
    "record_id",
    "company",
    "job_title",
    "location",
    "salary_usd",
    "years_experience",
    "date_posted",
    "skills",
]
NULL_TOKENS = ["", "NA", "N/A", "null", "None", "nan"]


def _next_record_id(path: Path) -> int:
    if not path.exists():
        return 1
    
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        if not rows:
            return 1
        return int(rows[-1]["record_id"]) + 1


def _generate_record(record_id: int) -> dict:
    posted_date = datetime.now() - timedelta(days=random.randint(0, 60))
    low_salary = random.randint(60000, 130000)
    high_salary = low_salary + random.randint(5000, 40000)

    return {
        "record_id": record_id,
        "company": random.choice(COMPANIES),
        "job_title": random.choice(TITLES),
        "location": random.choice(LOCATIONS),
        "salary_usd": f"{low_salary}-{high_salary}",
        "years_experience": random.randint(0, 10),
        "date_posted": posted_date.strftime("%Y-%m-%d"),
        "skills": ", ".join(random.sample(SKILLS, k=3)),
    }


def _inject_null_value(record: dict) -> None:
    nullable_fields = [
        "company",
        "job_title",
        "location",
        "salary_usd",
        "years_experience",
        "date_posted",
        "skills",
    ]
    target = random.choice(nullable_fields)
    record[target] = random.choice(NULL_TOKENS)


def _inject_format_error(record: dict) -> None:
    error_type = random.choice(
        [
            "invalid_salary",
            "invalid_date",
            "bad_experience",
            "no_skills_separator",
            "extra_whitespace",
        ]
    )

    if error_type == "invalid_salary":
        record["salary_usd"] = random.choice(["70000_to_90000", "eighty-thousand", "120000-90000", "-5000"])
    elif error_type == "invalid_date":
        record["date_posted"] = random.choice(["2026/12/01", "12-01-2026", "2026-13-40", "yesterday"])
    elif error_type == "bad_experience":
        record["years_experience"] = random.choice(["-2", "fifteen", "120"])
    elif error_type == "no_skills_separator":
        record["skills"] = " ".join(random.sample(SKILLS, k=3))
    elif error_type == "extra_whitespace":
        record["company"] = f"  {record['company']}  "
        record["job_title"] = f" {record['job_title']}    "


def _maybe_corrupt_record(record: dict) -> dict:
    if random.random() < NULL_RATE:
        _inject_null_value(record)
    if random.random() < ERROR_RATE:
        _inject_format_error(record)
    return record


def add_synthetic_records(path: Path = OUTPUT_FILE, count: int = RECORDS_TO_ADD) -> None:
    start_id = _next_record_id(path)
    records = []

    for i in range(count):
        new_record = _generate_record(start_id + i)

        if records and random.random() < CONTENT_DUPLICATE_RATE:
            duplicate = records[random.randint(0, len(records) - 1)].copy()
            duplicate["record_id"] = new_record["record_id"]
            new_record = duplicate

        new_record = _maybe_corrupt_record(new_record)
        records.append(new_record)

    if len(records) > 1:
        duplicate_id_count = max(1, int(count * RECORD_ID_DUPLICATE_RATE))
        for _ in range(duplicate_id_count):
            source_index = random.randint(0, len(records) - 1)
            target_index = random.randint(0, len(records) - 1)
            if source_index != target_index:
                records[target_index]["record_id"] = records[source_index]["record_id"]

    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)

    print(f"Added {count} records to {path.resolve()} with intentional data quality issues")


if __name__ == "__main__":
    add_synthetic_records()
