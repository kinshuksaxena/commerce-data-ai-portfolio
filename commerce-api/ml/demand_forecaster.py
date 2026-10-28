"""
Demand Forecaster Module for Commerce API.
This module interfaces with the primary ML pipeline in commerce-ml/pipelines/demand_forecaster.py
while maintaining seamless backwards compatibility for all API routes and background workers.
"""

import os
import sys

# Add commerce-ml/pipelines to sys.path
_COMMERCE_ML_PIPELINES = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "commerce-ml", "pipelines")
)
if os.path.exists(_COMMERCE_ML_PIPELINES) and _COMMERCE_ML_PIPELINES not in sys.path:
    sys.path.insert(0, _COMMERCE_ML_PIPELINES)

from demand_forecaster import (
    CommerceDemandForecaster,
    SuperKartDemandForecaster,
    CATALOG_DEFAULTS,
    DEFAULT_MODEL_PATH,
    demand_forecaster,
)

__all__ = [
    "CommerceDemandForecaster",
    "SuperKartDemandForecaster",
    "CATALOG_DEFAULTS",
    "DEFAULT_MODEL_PATH",
    "demand_forecaster",
]
