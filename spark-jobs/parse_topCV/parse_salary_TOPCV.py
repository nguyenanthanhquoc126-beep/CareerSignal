from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
)

PARSER_VERSION = "salary_parser_v4"

UNIT_CONFIG = {
    "triệu": {
        "currency": "VND",
        "multiplier": 1_000_000,
    },
    "tr": {
        "currency": "VND",
        "multiplier": 1_000_000,
    },
    "usd": {
        "currency": "USD",
        "multiplier": 1,
    },
}

salary_schema = StructType([
    StructField("min_amount", DoubleType(), True),
    StructField("max_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("period", StringType(), True),
    StructField("salary_type", StringType(), True),
    StructField("parse_status", StringType(), False),
    StructField("parser_version", StringType(), False),
    StructField("parse_error", StringType(), True),
])

def parse_range_salary(tokens):
    separator_index = tokens.index("-")

    if separator_index < 1 or separator_index + 2 >= len(tokens):
        return (None, None, None, None, "range", "unparsed", PARSER_VERSION, "missing_number_or_unit")

    min_text = tokens[separator_index - 1]
    max_text = tokens[separator_index + 1]
    unit = tokens[separator_index + 2]

    if unit not in UNIT_CONFIG:
        return (None, None, None, None, "range", "unparsed", PARSER_VERSION, f"unsupported_unit:{unit}")

    unit_config = UNIT_CONFIG[unit]

    try:
        if unit == "usd":
            min_value = float(min_text.replace(",", ""))
            max_value = float(max_text.replace(",", ""))
        else:
            min_value = float(min_text.replace(",", "."))
            max_value = float(max_text.replace(",", "."))
    except ValueError:
        return (None, None, None, None, "range", "unparsed", PARSER_VERSION, "invalid_number")

    if min_value > max_value:
        return (None, None, unit_config["currency"], None, "range", "invalid", PARSER_VERSION, "min_greater_than_max")

    return (
        min_value * unit_config["multiplier"],
        max_value * unit_config["multiplier"],
        unit_config["currency"],
        None,
        "range",
        "success",
        PARSER_VERSION,
        None,
    )
def parse_negotiable_salary(tokens):
    return (
        None,
        None,
        None,
        None,
        "negotiable",
        "success",
        PARSER_VERSION,
        None,
    )
def parse_up_to_salary(tokens):
    if len(tokens) < 3:
        return (None, None, None, None, "maximum", "unparsed", PARSER_VERSION, "missing_number_or_unit")

    amount_text = tokens[1]
    unit = tokens[2]

    if unit not in UNIT_CONFIG:
        return (None, None, None, None, "maximum", "unparsed", PARSER_VERSION, f"unsupported_unit:{unit}")

    unit_config = UNIT_CONFIG[unit]

    try:
        if unit == "usd":
            amount = float(amount_text.replace(",", ""))
        else:
            amount = float(amount_text.replace(",", "."))
    except ValueError:
        return (None, None, None, None, "maximum", "unparsed", PARSER_VERSION, "invalid_number")

    amount = amount * unit_config["multiplier"]

    return (amount, amount, unit_config["currency"], None, "maximum", "success", PARSER_VERSION, None)


def parse_from_salary(tokens):
    if len(tokens) < 3:
        return (None, None, None, None, "minimum", "unparsed", PARSER_VERSION, "missing_number_or_unit")

    amount_text = tokens[1]
    unit = tokens[2]

    if unit not in UNIT_CONFIG:
        return (None, None, None, None, "minimum", "unparsed", PARSER_VERSION, f"unsupported_unit:{unit}")

    unit_config = UNIT_CONFIG[unit]

    try:
        if unit == "usd":
            min_amount = float(amount_text.replace(",", ""))
        else:
            min_amount = float(amount_text.replace(",", "."))
    except ValueError:
        return (None, None, None, None, "minimum", "unparsed", PARSER_VERSION, "invalid_number")

    min_amount = min_amount * unit_config["multiplier"]

    return (min_amount, None, unit_config["currency"], None, "minimum", "success", PARSER_VERSION, None)


def parse_salary_text(raw_salary):
    if raw_salary is None or not raw_salary.strip():
        return (None, None, None, None, "unknown", "unparsed", PARSER_VERSION, "empty_salary")

    text = raw_salary.lower().strip()
    text = text.replace("–", "-")
    text = text.replace("—", "-")
    tokens = text.replace("-", " - ").split()

    if len(tokens) >= 2 and tokens[0] in ["thoả", "thỏa"] and tokens[1] == "thuận":
        return parse_negotiable_salary(tokens)

    if tokens[0] == "tới":
        return parse_up_to_salary(tokens)

    if tokens[0] == "từ":
        return parse_from_salary(tokens)

    if "-" in tokens:
        return parse_range_salary(tokens)

    return (None, None, None, None, "unknown", "unparsed", PARSER_VERSION, "unsupported_format")

parse_salary_udf = F.udf(
    parse_salary_text,
    salary_schema,
)

def apply_salary_parser(salary_column):
    return parse_salary_udf(salary_column)
