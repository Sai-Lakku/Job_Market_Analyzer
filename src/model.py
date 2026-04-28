import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

DATA_PATH = "Data/clean_data.csv"

ALL_SKILLS = [
    "Python", "SQL", "Tableau", "Power BI", "AWS",
    "Spark", "Pandas", "TensorFlow", "Airflow", "dbt",
]

# Feature Engineering

def parse_salary_midpoint(salary_str):
    low, high = str(salary_str).split("-")
    return (float(low) + float(high)) / 2


def multihot_skills(skills_str):
    present = {s.strip() for s in str(skills_str).split(",")}
    return {skill: int(skill in present) for skill in ALL_SKILLS}


def build_features(df):
    # Target: salary midpoint
    df = df.copy()
    df["salary_mid"] = df["salary_usd"].apply(parse_salary_midpoint)

    # Multi-hot encode skills
    skills_df = df["skills"].apply(multihot_skills).apply(pd.Series)
    df = pd.concat([df, skills_df], axis=1)

    # Month from posting date (captures recency/seasonality)
    df["month_posted"] = pd.to_datetime(df["date_posted"]).dt.month

    # One-hot encode categorical columns
    df = pd.get_dummies(df, columns=["job_title", "location", "company"])

    drop_cols = ["record_id", "salary_usd", "date_posted", "skills"]
    feature_cols = [c for c in df.columns if c not in drop_cols + ["salary_mid"]]

    X = df[feature_cols]
    y = df["salary_mid"]
    return X, y, feature_cols


# Evaluation helper

def evaluate(name, y_true, y_pred):
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    print(f"\n{name}")
    print(f"  MAE  : ${mae:>10,.0f}")
    print(f"  RMSE : ${rmse:>10,.0f}")
    print(f"  R²   :  {r2:.4f}")
    return {"MAE": mae, "RMSE": rmse, "R2": r2}


# Main

def main():
    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df):,} records with {df.shape[1]} columns.")

    X, y, feature_cols = build_features(df)
    print(f"Feature matrix: {X.shape[0]:,} rows x {X.shape[1]} features")
    print(f"Target (salary midpoint) — min: ${y.min():,.0f}  max: ${y.max():,.0f}  mean: ${y.mean():,.0f}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Random Forest
    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    rf_metrics = evaluate("Random Forest Regressor", y_test, rf.predict(X_test))

    # Feature Importance Plot 
    importances = (
        pd.Series(rf.feature_importances_, index=feature_cols)
        .sort_values(ascending=False)
        .head(20)
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    importances[::-1].plot(kind="barh", ax=ax, color="steelblue")
    ax.set_title("Top 20 Feature Importances — Random Forest Salary Predictor")
    ax.set_xlabel("Importance Score")
    plt.tight_layout()
    plt.savefig("feature_importance.png", dpi=150)
    print("\nFeature importance plot saved to feature_importance.png")
    plt.show()

    # Prediction vs Actual scatter 
    y_pred_rf = rf.predict(X_test)
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(y_test, y_pred_rf, alpha=0.3, s=10, color="steelblue")
    lims = [y_test.min(), y_test.max()]
    ax.plot(lims, lims, "r--", linewidth=1, label="Perfect prediction")
    ax.set_xlabel("Actual Salary Midpoint ($)")
    ax.set_ylabel("Predicted Salary Midpoint ($)")
    ax.set_title("Predicted vs Actual Salary — Random Forest")
    ax.legend()
    plt.tight_layout()
    plt.savefig("predicted_vs_actual.png", dpi=150)
    print("Predicted vs actual plot saved to predicted_vs_actual.png")
    plt.show()


if __name__ == "__main__":
    main()
