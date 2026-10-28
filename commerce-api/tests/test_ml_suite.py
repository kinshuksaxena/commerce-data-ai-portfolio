import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "commerce-agent")))

from ml.demand_forecaster import demand_forecaster
from ml.delay_risk_classifier import delay_classifier
from rag.catalog_rag import retrieve_relevant_products


def test_commerce_demand_forecast():
    res = demand_forecaster.predict_demand("PROD-001", horizon_days=14)
    assert res["status"] if "status" in res else True
    assert res["product_id"] == "PROD-001"
    assert res["total_forecasted_units"] > 0
    assert res["total_projected_revenue"] > 0
    assert len(res["forecast_daily"]) == 14
    assert "day_of_week" in res["feature_importance"]


def test_medical_assistant_hybrid_rag():
    res = retrieve_relevant_products("ergonomic silent keyboard for programming wrist pain", top_k=2)
    assert len(res) > 0
    top_match = res[0]
    assert "Keyboard" in top_match["product_name"]
    assert "similarity_score" in top_match
    assert top_match["similarity_score"] > 0.3


def test_delay_risk_classifier():
    risk_low = delay_classifier.predict_risk(stops_away=0, order_status="DELIVERED")
    assert risk_low["risk_level"] == "NONE"
    assert risk_low["on_time_probability"] == 1.0

    risk_high = delay_classifier.predict_risk(stops_away=6, order_status="OUT_FOR_DELIVERY", current_stop_number=1, destination_stop_number=7)
    assert risk_high["risk_level"] in ("MEDIUM", "HIGH")
    assert risk_high["delay_probability"] > 0.4
