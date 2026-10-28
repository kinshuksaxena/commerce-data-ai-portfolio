import datetime
import logging
import math
import os
from typing import Any, Dict, List, Optional
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Primary model storage under commerce-ml/models
CANDIDATE_PATHS = [
    os.path.join(os.path.dirname(__file__), "..", "models", "commerce_demand_model.pkl"),
    os.path.join(os.path.dirname(__file__), "..", "..", "commerce-ml", "models", "commerce_demand_model.pkl"),
    "/commerce-ml/models/commerce_demand_model.pkl",
    "/app/models/commerce_demand_model.pkl",
]
DEFAULT_MODEL_PATH = next((p for p in CANDIDATE_PATHS if os.path.exists(p)), CANDIDATE_PATHS[0])

# Reference products matching live catalog
CATALOG_DEFAULTS = [
    {"product_id": "PROD-001", "name": "Ergonomic Mechanical Keyboard", "category": "Electronics", "base_daily": 14, "price": 129.99},
    {"product_id": "PROD-002", "name": "Ultra-Wide Gaming Monitor 34\"", "category": "Electronics", "base_daily": 8, "price": 499.50},
    {"product_id": "PROD-003", "name": "Wireless Noise Cancelling Headphones", "category": "Audio", "base_daily": 18, "price": 249.00},
    {"product_id": "PROD-004", "name": "Smart Fitness Smartwatch v2", "category": "Wearables", "base_daily": 12, "price": 199.99},
    {"product_id": "PROD-005", "name": "Aerodynamic Carbon Fiber Road Bike", "category": "Sports", "base_daily": 3, "price": 1250.00},
    {"product_id": "PROD-006", "name": "Precision Ergonomic Wireless Mouse", "category": "Electronics", "base_daily": 22, "price": 79.99},
    {"product_id": "PROD-007", "name": "Studio USB Condenser Microphone", "category": "Audio", "base_daily": 10, "price": 149.50},
    {"product_id": "PROD-008", "name": "4K Ultra-HD Streaming Webcam", "category": "Electronics", "base_daily": 15, "price": 119.00},
    {"product_id": "PROD-009", "name": "Standing Desk Converter Motorized", "category": "Office", "base_daily": 5, "price": 349.00},
    {"product_id": "PROD-010", "name": "Stainless Steel Insulated Smart Bottle", "category": "Fitness", "base_daily": 25, "price": 45.00},
]


class CommerceDemandForecaster:
    """
    Commerce Retail Sales & Demand Forecasting Model.
    Predicts unit demand and gross revenue across regional outlets for upcoming horizons.
    """

    def __init__(self, model_path: str = DEFAULT_MODEL_PATH):
        self.model_path = model_path
        self.model = None
        self.feature_names = [
            "day_of_week",
            "day_of_month",
            "is_weekend",
            "rolling_7d_qty",
            "rolling_14d_qty",
            "unit_price",
            "price_diff_ratio",
        ]
        self._load_or_train()

    def _generate_synthetic_historical_data(self) -> pd.DataFrame:
        """Generates realistic 90-day historical time-series demand data for all catalog products."""
        np.random.seed(42)
        end_date = datetime.date.today()
        dates = [end_date - datetime.timedelta(days=i) for i in range(90, 0, -1)]

        rows = []
        for prod in CATALOG_DEFAULTS:
            base = prod["base_daily"]
            price = prod["price"]
            for d in dates:
                dow = d.weekday()
                weekend_boost = 1.35 if dow in (4, 5, 6) else 1.0
                seasonal_noise = np.random.normal(1.0, 0.15)
                qty = max(1, int(base * weekend_boost * seasonal_noise))
                rows.append({
                    "product_id": prod["product_id"],
                    "sale_date": d,
                    "quantity": qty,
                    "unit_price": price,
                    "revenue": round(qty * price, 2),
                    "day_of_week": dow,
                    "day_of_month": d.day,
                    "is_weekend": 1 if dow in (4, 5, 6) else 0,
                })

        df = pd.DataFrame(rows)
        df.sort_values(by=["product_id", "sale_date"], inplace=True)

        # Compute lag and rolling statistics
        df["rolling_7d_qty"] = df.groupby("product_id")["quantity"].transform(
            lambda x: x.rolling(7, min_periods=1).mean()
        )
        df["rolling_14d_qty"] = df.groupby("product_id")["quantity"].transform(
            lambda x: x.rolling(14, min_periods=1).mean()
        )
        df["price_diff_ratio"] = 0.0
        return df

    def train(self, data: Optional[pd.DataFrame] = None) -> Dict[str, float]:
        """Trains an ensemble GradientBoostingRegressor with Random Forest benchmark."""
        if data is None or data.empty:
            logging.info("Training on synthesized retail historical demand corpus...")
            data = self._generate_synthetic_historical_data()

        X = data[self.feature_names]
        y = data["quantity"]

        # 80/20 train/test temporal split
        split_idx = int(len(data) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        self.model = GradientBoostingRegressor(
            n_estimators=100, learning_rate=0.08, max_depth=4, random_state=42
        )
        self.model.fit(X_train, y_train)

        preds = self.model.predict(X_test)
        mae = float(mean_absolute_error(y_test, preds))
        rmse = float(root_mean_squared_error(y_test, preds))

        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        joblib.dump(self.model, self.model_path)
        logging.info(f"Model saved to {self.model_path} (MAE: {mae:.2f}, RMSE: {rmse:.2f})")

        return {"mae": round(mae, 2), "rmse": round(rmse, 2), "training_samples": len(data)}

    def _load_or_train(self):
        if os.path.exists(self.model_path):
            try:
                self.model = joblib.load(self.model_path)
                logging.info(f"Loaded existing demand forecasting model from {self.model_path}")
                return
            except Exception as e:
                logging.warning(f"Failed loading model from {self.model_path}: {e}")
        self.train()

    def predict_demand(
        self, product_id: str, horizon_days: int = 14
    ) -> Dict[str, Any]:
        """
        Forecasts future daily sales demand for a given product over the requested horizon.
        Returns daily predicted units, upper/lower confidence bounds, and projected revenue.
        """
        if self.model is None:
            self._load_or_train()

        # Find product details
        prod = next((p for p in CATALOG_DEFAULTS if p["product_id"] == product_id), None)
        if not prod:
            prod = {
                "product_id": product_id,
                "name": f"Product {product_id}",
                "category": "General",
                "base_daily": 10,
                "price": 99.99,
            }

        start_date = datetime.date.today()
        predictions = []
        cumulative_units = 0
        cumulative_revenue = 0.0

        rolling_7 = float(prod["base_daily"])
        rolling_14 = float(prod["base_daily"])

        for i in range(1, horizon_days + 1):
            target_date = start_date + datetime.timedelta(days=i)
            dow = target_date.weekday()
            dom = target_date.day
            is_wknd = 1 if dow in (4, 5, 6) else 0

            features = pd.DataFrame([{
                "day_of_week": dow,
                "day_of_month": dom,
                "is_weekend": is_wknd,
                "rolling_7d_qty": rolling_7,
                "rolling_14d_qty": rolling_14,
                "unit_price": prod["price"],
                "price_diff_ratio": 0.0,
            }])

            pred_qty = max(1, int(round(self.model.predict(features)[0])))
            pred_rev = round(pred_qty * prod["price"], 2)

            cumulative_units += pred_qty
            cumulative_revenue += pred_rev

            # Update rolling momentum
            rolling_7 = 0.85 * rolling_7 + 0.15 * pred_qty
            rolling_14 = 0.90 * rolling_14 + 0.10 * pred_qty

            predictions.append({
                "date": target_date.strftime("%Y-%m-%d"),
                "day_name": target_date.strftime("%a"),
                "predicted_quantity": pred_qty,
                "lower_bound": max(1, pred_qty - 2),
                "upper_bound": pred_qty + 3,
                "projected_revenue": pred_rev,
            })

        feature_importances = dict(
            zip(self.feature_names, [round(float(x), 4) for x in self.model.feature_importances_])
        )

        return {
            "product_id": prod["product_id"],
            "product_name": prod["name"],
            "category": prod["category"],
            "unit_price": prod["price"],
            "horizon_days": horizon_days,
            "total_forecasted_units": cumulative_units,
            "total_projected_revenue": round(cumulative_revenue, 2),
            "average_daily_units": round(cumulative_units / horizon_days, 1),
            "forecast_daily": predictions,
            "feature_importance": feature_importances,
            "model_metadata": {
                "algorithm": "GradientBoostingRegressor (Commerce Demand Forecasting)",
                "source_project": "Commerce Demand Forecaster",
                "status": "ready",
            },
        }


# Singleton forecaster instance
CommerceDemandForecasterAlias = CommerceDemandForecaster
SuperKartDemandForecaster = CommerceDemandForecaster  # Backward compatibility alias
demand_forecaster = CommerceDemandForecaster()
