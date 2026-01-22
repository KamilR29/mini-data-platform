import os
import json
import pandas as pd

import great_expectations as gx

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.getenv("S3_KEY", "minio")
S3_SECRET = os.getenv("S3_SECRET", "minio12345")

SILVER_PATH = "s3://silver/customers/"
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def load_silver_customers() -> pd.DataFrame:
    return pd.read_parquet(
        SILVER_PATH,
        storage_options={
            "key": S3_KEY,
            "secret": S3_SECRET,
            "client_kwargs": {"endpoint_url": S3_ENDPOINT},
        },
    )


def main():
    df = load_silver_customers()

    context = gx.get_context()

    datasource = context.sources.add_or_update_pandas(name="pandas")

    asset = datasource.add_dataframe_asset(name="silver_customers_asset")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "customers_silver_suite"
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Dopasuj do Twoich kolumn w silver:
    # u Ciebie w silver są: customer_id, age, country, gender, annual_income_usd, total_orders, avg_order_value, churn, op, ts_ms
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("customer_id")
    validator.expect_column_values_to_not_be_null("email") if "email" in df.columns else None

    # Email tylko jeśli istnieje kolumna
    if "email" in df.columns:
        validator.expect_column_values_to_match_regex("email", EMAIL_REGEX)

    result = validator.validate()

    os.makedirs("qa/reports", exist_ok=True)
    with open("qa/reports/customers_validation.json", "w", encoding="utf-8") as f:
        json.dump(result.to_json_dict(), f, ensure_ascii=False, indent=2)

    print("SUCCESS:", result.success)

    if not result.success:
        for r in result.results:
            if not r.success:
                print("FAILED:", r.expectation_config.expectation_type, r.expectation_config.kwargs)

    raise SystemExit(0 if result.success else 1)


if __name__ == "__main__":
    main()
