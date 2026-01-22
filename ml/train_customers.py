import os
import pandas as pd

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
S3_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minio12345")

SILVER_PATH = "s3://silver/customers/"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "customers_churn")
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "CustomerChurnModel")


def load_silver() -> pd.DataFrame:
    return pd.read_parquet(
        SILVER_PATH,
        storage_options={
            "key": S3_KEY,
            "secret": S3_SECRET,
            "client_kwargs": {"endpoint_url": S3_ENDPOINT},
        },
    )


def main() -> None:
    df = load_silver()

    # minimalne sanity: churn musi istnieć
    if "churn" not in df.columns:
        raise SystemExit("Brak kolumny 'churn' w Silver. Nie mam na czym trenować.")

    # Feature set, dopasowany do tego co masz w silver
    feature_cols = [c for c in ["age", "country", "gender", "annual_income_usd", "total_orders", "avg_order_value"] if c in df.columns]
    target_col = "churn"

    data = df[feature_cols + [target_col]].dropna(subset=[target_col]).copy()

    X = data[feature_cols]
    y = data[target_col].astype(int)

    cat_cols = [c for c in ["country", "gender"] if c in feature_cols]
    num_cols = [c for c in feature_cols if c not in cat_cols]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]), num_cols),
            ("cat", Pipeline(steps=[("imputer", SimpleImputer(strategy="most_frequent")),
                                   ("ohe", OneHotEncoder(handle_unknown="ignore"))]), cat_cols),
        ],
        remainder="drop",
    )

    model = LogisticRegression(max_iter=200)

    pipe = Pipeline(steps=[("prep", preprocessor), ("model", model)])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.nunique() > 1 else None
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name="logreg_customers_churn"):
        mlflow.log_param("model_type", "LogisticRegression")
        mlflow.log_param("features", ",".join(feature_cols))

        pipe.fit(X_train, y_train)

        y_pred = pipe.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, zero_division=0)

        mlflow.log_metric("accuracy", float(acc))
        mlflow.log_metric("f1", float(f1))

        # AUC tylko jeśli są dwie klasy i da się policzyć proby
        if y.nunique() == 2:
            proba = pipe.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, proba)
            mlflow.log_metric("roc_auc", float(auc))

        mlflow.sklearn.log_model(
            sk_model=pipe,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )

    print("OK: training + logging done")


if __name__ == "__main__":
    main()
