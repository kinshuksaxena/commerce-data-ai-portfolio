# Commerce Machine Learning Subsystem (`commerce-ml`)

This module houses the machine learning models, training and inference pipelines, reference studies, and interactive Jupyter notebooks integrated from Kinshuk Saxena's **UT Austin Post Graduate Program in Artificial Intelligence & Machine Learning**.

---

## 📁 Directory Structure

```
commerce-ml/
├── data/                       # Telematics & delivery logistics datasets
│   ├── delivery_delay_dataset.csv
│   └── commerce_demand_dataset.csv
├── models/                     # Serialized production models
│   ├── commerce_demand_model.pkl
│   └── delivery_delay_model.pkl
├── notebooks/                  # Interactive Jupyter analysis & training notebooks
│   ├── delay_risk_classifier.ipynb                # Supervised Delivery Delay ML Pipeline (EDA, ROC-AUC, Calibrated GB)
│   ├── commerce_demand_forecasting.ipynb          # Commerce Retail Demand Forecasting (Ensemble Regression, Seasonality, Elasticity)
│   ├── 02_medical_assistant_hybrid_rag.ipynb       # Medical Assistant Advanced Hybrid RAG (Vector + BM25/Cosine)
│   ├── 03_easyvisa_delay_risk_classifier.ipynb    # EasyVisa Ensemble Classification (Class Imbalance & Risk Scoring)
│   └── 04_renewind_predictive_maintenance.ipynb    # ReneWind Sensor Failure Prediction & Predictive Maintenance
├── pipelines/                  # Production inference & feature extraction pipelines
│   ├── demand_forecaster.py    # GradientBoostingRegressor for unit demand & revenue bounds
│   └── delay_risk_classifier.py# CalibratedClassifierCV(GradientBoosting) evaluating telematics & congestion
└── reference/                  # Original UT Austin project reference archives
    ├── ks_project3.html        # EasyVisa Visa Approval Classification
    ├── ks_project4.html        # ReneWind Wind Turbine Failure Prediction
    ├── ks_project5.html        # Medical Assistant RAG LLM System
    └── ks_project7.html        # SuperKart Retail Sales Forecasting & Deployment
```

---

## 🚀 Interactive Notebooks Summary

| Notebook | Domain | Techniques & Models | Production Application |
| :--- | :--- | :--- | :--- |
| **`commerce_demand_forecasting.ipynb`** | Retail Analytics | Time-series feature engineering, Seasonality decomposition, Decision Trees, Gradient Boosting Regressor, Hyperparameter Tuning | Powers Commerce API `/api/ml/forecast/demand` & Portal Demand Forecaster UI |
| **`delay_risk_classifier.ipynb`** | Telematics Logistics | Synthetic telemetry, EDA, Logistic Regression, Random Forest, Calibrated Gradient Boosting (`CalibratedClassifierCV`) | Powers Commerce API `/api/ml/predict/delay-risk` & Delivery Risk Scoring |
| **`02_medical_assistant_hybrid_rag.ipynb`** | GenAI & Knowledge Retrieval | Document chunking, Sentence Transformers / Vector Retrieval, Cosine Similarity, Prompt Engineering | Powers Agentic AI Hybrid Catalog & Policy RAG (`BrowseAgent`) |
| **`03_easyvisa_delay_risk_classifier.ipynb`** | Mobility & Risk Analysis | Oversampling / Undersampling, AdaBoost, Logistic Classification, Feature Importance | Reference study on classification imbalance |
| **`04_renewind_predictive_maintenance.ipynb`** | IoT & Sensor Reliability | Missing value imputation, Ensemble tuning, ROC-AUC optimization, Cost-sensitive learning | Telematics & sensor anomaly detection across delivery fleet |

---

## 🛠️ Usage

### Running Tests
```bash
uv run pytest commerce-api/tests/test_ml_suite.py
```

### Starting Jupyter Notebooks
```bash
uv run jupyter lab commerce-ml/notebooks/
```
