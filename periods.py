# periods.py
from datetime import datetime

def generate_monthly(start_year, end_year, end_month=12):
    for y in range(start_year, end_year + 1):
        last = end_month if y == end_year else 12
        for m in range(1, last + 1):
            yield f"{y}{m:02d}"

def generate_quarterly(start_year, end_year, end_quarter=4):
    for y in range(start_year, end_year + 1):
        last = end_quarter if y == end_year else 4
        for q in range(1, last + 1):
            yield f"{y}Q{q}"

def generate_yearly(start_year, end_year):
    for y in range(start_year, end_year + 1):
        yield str(y)

def generate_periods(period_type, start_year, end_year, end_month=12):
    pt = period_type.lower()
    if pt == "monthly":
        return list(generate_monthly(start_year, end_year, end_month))
    elif pt == "quarterly":
        return list(generate_quarterly(start_year, end_year))
    elif pt == "yearly":
        return list(generate_yearly(start_year, end_year))
    else:
        raise ValueError(f"Unsupported period type: {period_type}")
