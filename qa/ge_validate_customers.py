import os
import json
import pandas as pd
import great_expectations as gx

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minio12345")

SILVER_PATH = "s3://silver/customers/"


def load_silver_customers() -> pd.DataFrame:
    return pd.read_parquet(
        SILVER_PATH,
        storage_options={
            "key": S3_KEY,
            "secret": S3_SECRET,
            "client_kwargs": {"endpoint_url": S3_ENDPOINT},
        },
    )


def main() -> None:
    df = load_silver_customers()

   
    context = gx.get_context(context_root_dir="/work/great_expectations")


    suite_name = "customers_silver_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(suite_name)


    from great_expectations.core.batch import RuntimeBatchRequest

    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_pandas",
        data_connector_name="runtime_data_connector",
        data_asset_name="silver_customers_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )


    try:
        context.get_datasource("runtime_pandas")
    except Exception:
        context.add_datasource(
            name="runtime_pandas",
            class_name="Datasource",
            execution_engine={"class_name": "PandasExecutionEngine"},
            data_connectors={
                "runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                }
            },
        )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    expected_cols = [
        "customer_id",
        "age",
        "country",
        "gender",
        "annual_income_usd",
        "total_orders",
        "avg_order_value",
        "churn",
        "op",
        "ts_ms",
    ]



    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    validator.expect_table_columns_to_match_ordered_list(expected_cols)

    validator.expect_column_values_to_not_be_null("customer_id")


    validator.expect_column_values_to_be_unique("customer_id")


    validator.expect_column_values_to_be_between("age", min_value=0, max_value=120, mostly=0.99)
    validator.expect_column_values_to_be_in_set("gender", ["Male", "Female"], mostly=0.95)

    validator.expect_column_values_to_be_between("annual_income_usd", min_value=0, max_value=None, mostly=0.99)
    validator.expect_column_values_to_be_between("total_orders", min_value=0, max_value=None, mostly=0.99)
    validator.expect_column_values_to_be_between("avg_order_value", min_value=0, max_value=None, mostly=0.99)

    validator.expect_column_values_to_be_in_set("churn", [0, 1], mostly=0.99)
    validator.expect_column_values_to_be_in_set("op", ["c", "u", "d", "r"], mostly=0.95)

    if "ts_ms" in df.columns:
        non_null_ratio = float(df["ts_ms"].notna().mean())
        if non_null_ratio > 0.0:
            validator.expect_column_values_to_not_be_null("ts_ms", mostly=0.95)
            validator.expect_column_values_to_be_between("ts_ms", min_value=0, max_value=None, mostly=0.95)
        else:
            print("WARN: ts_ms is 100% NULL, skipping ts_ms expectations.")

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
