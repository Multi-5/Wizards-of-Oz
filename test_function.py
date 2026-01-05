import os
import sys
sys.path.append('airflow/dags')

# Set paths before importing
LANDING_PARQUET = "data/landing/food.parquet"
STAGING_OFF = "data/staging/openfoodfacts_cleaned_test.parquet"

# Ensure directories
os.makedirs(os.path.dirname(LANDING_PARQUET), exist_ok=True)
os.makedirs(os.path.dirname(STAGING_OFF), exist_ok=True)

# Now import and modify
import staging_pipeline
staging_pipeline.LANDING_PARQUET = LANDING_PARQUET
staging_pipeline.STAGING_OFF = STAGING_OFF

# Call the function
staging_pipeline.clean_openfoodfacts_data()

print("Function executed successfully.")