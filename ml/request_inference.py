import requests

payload = {
    "dataframe_records": [
        {
            "age": 31,
            "country": "Poland",
            "gender": "Male",
            "annual_income_usd": 42000.0,
            "total_orders": 12,
            "avg_order_value": 55.5
        }
    ]
}

r = requests.post("http://localhost:6000/invocations", json=payload, timeout=30)
print(r.status_code)
print(r.text)
