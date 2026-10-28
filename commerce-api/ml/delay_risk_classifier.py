"""
Delivery Delay Risk Classifier Module for Commerce API.
This module interfaces with the primary ML pipeline in commerce-ml/pipelines/delay_risk_classifier.py
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

from delay_risk_classifier import (
    EasyVisaDelayClassifier,
    delay_classifier,
)

__all__ = [
    "EasyVisaDelayClassifier",
    "delay_classifier",
]
