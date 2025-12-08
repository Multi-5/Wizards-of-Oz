"""
Production Pipeline - Staging to Production Zone with Star Schema

This pipeline:
1. Loads enriched data from staging zone
2. Transforms data into Star Schema (Fact + Dimension tables)
3. Loads data into PostgreSQL production database
4. Creates analytical views (Data Marts) for the 5 research questions
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

# Paths
STAGING_DIR = '/opt/airflow/data/staging'
PRODUCTION_DIR = '/opt/airflow/data/production'
ENRICHED_FILE = os.path.join(STAGING_DIR, 'enriched_food_data.parquet')


def _ensure_directories(paths: set[str]) -> None:
    for path in paths:
        if not path:
            continue
        os.makedirs(path, exist_ok=True)


_ensure_directories({
    STAGING_DIR,
    PRODUCTION_DIR,
    os.path.dirname(ENRICHED_FILE),
})

# Default args
default_args = {
    'owner': 'wizards_of_oz',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# DAG Definition
dag = DAG(
    'production_pipeline',
    default_args=default_args,
    description='Load data from staging to production with Star Schema',
    schedule=None,  # Manual trigger
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_tasks=1,
    tags=['production', 'star-schema', 'postgres'],
)


def get_db_connection():
    """Get direct psycopg2 connection to warehouse using env vars"""
    import psycopg2
    import os
    
    # Read from environment variables (set in docker-compose.yml from .env)
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        database=os.getenv('POSTGRES_DB', 'warehouse'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow')
    )


def create_star_schema():
    """
    Create Star Schema in PostgreSQL:
    - Fact Table: fact_food_nutrition
    - Dimension Tables: dim_food, dim_category, dim_source, dim_date
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop existing tables (for idempotency)
    drop_sql = """
    DROP TABLE IF EXISTS fact_food_nutrition CASCADE;
    DROP TABLE IF EXISTS dim_food CASCADE;
    DROP TABLE IF EXISTS dim_category CASCADE;
    DROP TABLE IF EXISTS dim_source CASCADE;
    DROP TABLE IF EXISTS dim_date CASCADE;
    """
    
    # Create Dimension Tables
    create_dimensions_sql = """
    -- Dimension: Food
    CREATE TABLE dim_food (
        food_key SERIAL PRIMARY KEY,
        food_id VARCHAR(255) UNIQUE NOT NULL,
        description TEXT NOT NULL,
        brands TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX idx_dim_food_id ON dim_food(food_id);
    
    -- Dimension: Category
    CREATE TABLE dim_category (
        category_key SERIAL PRIMARY KEY,
        category_name VARCHAR(255) UNIQUE NOT NULL,
        category_type VARCHAR(50)
    );
    CREATE INDEX idx_dim_category_name ON dim_category(category_name);
    
    -- Dimension: Source
    CREATE TABLE dim_source (
        source_key SERIAL PRIMARY KEY,
        source_name VARCHAR(100) UNIQUE NOT NULL,
        source_description TEXT
    );
    INSERT INTO dim_source (source_name, source_description) VALUES
        ('USDA', 'USDA FoodData Central - Foundation Foods'),
        ('OpenFoodFacts', 'OpenFoodFacts Community Database');
    
    -- Dimension: Date
    CREATE TABLE dim_date (
        date_key SERIAL PRIMARY KEY,
        full_date DATE UNIQUE NOT NULL,
        year INT,
        month INT,
        day INT,
        quarter INT,
        day_of_week INT
    );
    INSERT INTO dim_date (full_date, year, month, day, quarter, day_of_week)
    SELECT 
        CURRENT_DATE,
        EXTRACT(YEAR FROM CURRENT_DATE),
        EXTRACT(MONTH FROM CURRENT_DATE),
        EXTRACT(DAY FROM CURRENT_DATE),
        EXTRACT(QUARTER FROM CURRENT_DATE),
        EXTRACT(DOW FROM CURRENT_DATE);
    
    -- Fact Table: Food Nutrition
    CREATE TABLE fact_food_nutrition (
        nutrition_key SERIAL PRIMARY KEY,
        food_key INT REFERENCES dim_food(food_key),
        category_key INT REFERENCES dim_category(category_key),
        source_key INT REFERENCES dim_source(source_key),
        date_key INT REFERENCES dim_date(date_key),
        
        -- Nutritional Measures
        energy_kcal FLOAT,
        protein_g FLOAT,
        fat_g FLOAT,
        carbohydrate_g FLOAT,
        sugar_g FLOAT,
        fiber_g FLOAT,
        sodium_mg FLOAT,
        
        -- Vitamins
        vitamin_a_mcg FLOAT,
        vitamin_c_mg FLOAT,
        vitamin_d_mcg FLOAT,
        vitamin_e_mg FLOAT,
        
        -- Minerals
        calcium_mg FLOAT,
        iron_mg FLOAT,
        magnesium_mg FLOAT,
        potassium_mg FLOAT,
        zinc_mg FLOAT,
        
        -- Calculated Metrics
        vitamin_density FLOAT,
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX idx_fact_food_key ON fact_food_nutrition(food_key);
    CREATE INDEX idx_fact_category_key ON fact_food_nutrition(category_key);
    CREATE INDEX idx_fact_source_key ON fact_food_nutrition(source_key);
    """
    
    print("🏗️  Creating Star Schema...")
    cursor.execute(drop_sql)
    cursor.execute(create_dimensions_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Star Schema created successfully!")


def load_dimensions():
    """
    Load dimension tables from staging data
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Read enriched data
    df = pd.read_parquet(ENRICHED_FILE)
    
    print(f"📊 Loading {len(df)} records into dimension tables...")
    
    # Load dim_category
    categories = df[['category']].drop_duplicates().dropna()
    categories['category_type'] = 'food'
    
    for _, row in categories.iterrows():
        cursor.execute(
            "INSERT INTO dim_category (category_name, category_type) VALUES (%s, %s) ON CONFLICT (category_name) DO NOTHING",
            (row['category'], row['category_type'])
        )
    
    print(f"✅ Loaded {len(categories)} categories")
    
    # Load dim_food
    foods = df[['food_id', 'description', 'brands']].drop_duplicates()
    
    for _, row in foods.iterrows():
        cursor.execute(
            "INSERT INTO dim_food (food_id, description, brands) VALUES (%s, %s, %s) ON CONFLICT (food_id) DO NOTHING",
            (str(row['food_id']), row['description'], row.get('brands', None))
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(foods)} food items")
    print(f"✅ Dimensions loaded successfully!")


def load_fact_table():
    """
    Load fact table with nutritional data
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Read enriched data
    df = pd.read_parquet(ENRICHED_FILE)
    
    print(f"📊 Loading {len(df)} records into fact table...")
    
    # Get foreign keys
    for idx, row in df.iterrows():
        # Get food_key
        cursor.execute("SELECT food_key FROM dim_food WHERE food_id = %s", (str(row['food_id']),))
        food_key = cursor.fetchone()[0]
        
        # Get category_key
        category_key = None
        if pd.notna(row.get('category')):
            cursor.execute("SELECT category_key FROM dim_category WHERE category_name = %s", (row['category'],))
            category_result = cursor.fetchone()
            if category_result:
                category_key = category_result[0]
        
        # Get source_key
        cursor.execute("SELECT source_key FROM dim_source WHERE source_name = %s", (row['source'],))
        source_key = cursor.fetchone()[0]
        
        # Get date_key
        cursor.execute("SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE")
        date_key = cursor.fetchone()[0]
        
        # Insert into fact table
        cursor.execute("""
            INSERT INTO fact_food_nutrition (
                food_key, category_key, source_key, date_key,
                energy_kcal, protein_g, fat_g, carbohydrate_g, sugar_g, fiber_g, sodium_mg,
                vitamin_a_mcg, vitamin_c_mg, vitamin_d_mcg, vitamin_e_mg,
                calcium_mg, iron_mg, magnesium_mg, potassium_mg, zinc_mg,
                vitamin_density
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            food_key, category_key, source_key, date_key,
            row.get('energy_kcal', 0), row.get('protein_g', 0), row.get('fat_g', 0),
            row.get('carbohydrate_g', 0), row.get('sugar_g', 0), row.get('fiber_g', 0),
            row.get('sodium_mg', 0), row.get('vitamin_a_mcg', 0), row.get('vitamin_c_mg', 0),
            row.get('vitamin_d_mcg', 0), row.get('vitamin_e_mg', 0), row.get('calcium_mg', 0),
            row.get('iron_mg', 0), row.get('magnesium_mg', 0), row.get('potassium_mg', 0),
            row.get('zinc_mg', 0), row.get('vitamin_density', 0)
        ))
        
        if (idx + 1) % 100 == 0:
            print(f"  Loaded {idx + 1}/{len(df)} records...")
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Fact table loaded successfully!")


def create_data_marts():
    """
    Create analytical views (Data Marts) for the 5 research questions
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    views_sql = """
    -- Data Mart 1: Nutrient Comparison by Category
    CREATE OR REPLACE VIEW mart_nutrient_by_category AS
    SELECT 
        c.category_name,
        s.source_name,
        COUNT(*) as food_count,
        AVG(f.energy_kcal) as avg_energy_kcal,
        AVG(f.protein_g) as avg_protein_g,
        AVG(f.fat_g) as avg_fat_g,
        AVG(f.carbohydrate_g) as avg_carbohydrate_g,
        AVG(f.sugar_g) as avg_sugar_g,
        AVG(f.fiber_g) as avg_fiber_g,
        AVG(f.sodium_mg) as avg_sodium_mg
    FROM fact_food_nutrition f
    JOIN dim_category c ON f.category_key = c.category_key
    JOIN dim_source s ON f.source_key = s.source_key
    GROUP BY c.category_name, s.source_name;
    
    -- Data Mart 2: Sodium Content Analysis
    CREATE OR REPLACE VIEW mart_sodium_analysis AS
    SELECT 
        c.category_name,
        s.source_name,
        COUNT(*) as food_count,
        AVG(f.sodium_mg) as avg_sodium_mg,
        MIN(f.sodium_mg) as min_sodium_mg,
        MAX(f.sodium_mg) as max_sodium_mg,
        STDDEV(f.sodium_mg) as stddev_sodium_mg
    FROM fact_food_nutrition f
    JOIN dim_category c ON f.category_key = c.category_key
    JOIN dim_source s ON f.source_key = s.source_key
    GROUP BY c.category_name, s.source_name
    HAVING AVG(f.sodium_mg) > 0;
    
    -- Data Mart 3: Vitamin Density by Processing Level
    CREATE OR REPLACE VIEW mart_vitamin_density AS
    SELECT 
        c.category_name,
        COUNT(*) as food_count,
        AVG(f.vitamin_density) as avg_vitamin_density,
        AVG(f.vitamin_a_mcg) as avg_vitamin_a,
        AVG(f.vitamin_c_mg) as avg_vitamin_c,
        AVG(f.vitamin_d_mcg) as avg_vitamin_d,
        AVG(f.vitamin_e_mg) as avg_vitamin_e
    FROM fact_food_nutrition f
    JOIN dim_category c ON f.category_key = c.category_key
    GROUP BY c.category_name
    HAVING AVG(f.vitamin_density) > 0;
    
    -- Data Mart 4: Magnesium and Vitamin C Correlation
    CREATE OR REPLACE VIEW mart_magnesium_vitamin_c AS
    SELECT 
        c.category_name,
        COUNT(*) as food_count,
        AVG(f.magnesium_mg) as avg_magnesium_mg,
        AVG(f.vitamin_c_mg) as avg_vitamin_c_mg,
        CORR(f.magnesium_mg, f.vitamin_c_mg) as correlation_coef
    FROM fact_food_nutrition f
    JOIN dim_category c ON f.category_key = c.category_key
    WHERE f.magnesium_mg > 0 AND f.vitamin_c_mg > 0
    GROUP BY c.category_name
    HAVING COUNT(*) >= 10;
    
    -- Data Mart 5: Complete Nutrition Overview
    CREATE OR REPLACE VIEW mart_nutrition_overview AS
    SELECT 
        d.description as food_name,
        c.category_name,
        s.source_name,
        f.energy_kcal,
        f.protein_g,
        f.fat_g,
        f.carbohydrate_g,
        f.sugar_g,
        f.sodium_mg,
        f.vitamin_c_mg,
        f.magnesium_mg,
        f.vitamin_density
    FROM fact_food_nutrition f
    JOIN dim_food d ON f.food_key = d.food_key
    JOIN dim_category c ON f.category_key = c.category_key
    JOIN dim_source s ON f.source_key = s.source_key;
    """
    
    print("📊 Creating Data Marts (Analytical Views)...")
    cursor.execute(views_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data Marts created successfully!")


def backup_to_production():
    """
    Backup staging data to production zone (permanent storage)
    """
    import shutil
    
    os.makedirs(PRODUCTION_DIR, exist_ok=True)
    
    # Copy enriched data to production
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    production_file = os.path.join(PRODUCTION_DIR, f'enriched_food_data_{timestamp}.parquet')
    
    shutil.copy2(ENRICHED_FILE, production_file)
    
    # Also keep a "latest" version
    latest_file = os.path.join(PRODUCTION_DIR, 'enriched_food_data_latest.parquet')
    shutil.copy2(ENRICHED_FILE, latest_file)
    
    print(f"✅ Data backed up to production: {production_file}")


# Define Tasks
task_create_schema = PythonOperator(
    task_id='create_star_schema',
    python_callable=create_star_schema,
    dag=dag,
)

task_load_dimensions = PythonOperator(
    task_id='load_dimensions',
    python_callable=load_dimensions,
    dag=dag,
)

task_load_facts = PythonOperator(
    task_id='load_fact_table',
    python_callable=load_fact_table,
    dag=dag,
)

task_create_marts = PythonOperator(
    task_id='create_data_marts',
    python_callable=create_data_marts,
    dag=dag,
)

task_backup = PythonOperator(
    task_id='backup_to_production',
    python_callable=backup_to_production,
    dag=dag,
)

# Task Dependencies
task_create_schema >> task_load_dimensions >> task_load_facts >> task_create_marts >> task_backup
