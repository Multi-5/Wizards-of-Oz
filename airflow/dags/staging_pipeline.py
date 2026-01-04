import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

# Pfade
LANDING_JSON = "/opt/airflow/data/landing/FoodData_Central_foundation_food_json_2025-04-24.json"
LANDING_PARQUET = "/opt/airflow/data/landing/food.parquet"
STAGING_USDA = "/opt/airflow/data/staging/usda_cleaned.parquet"
STAGING_OFF = "/opt/airflow/data/staging/openfoodfacts_cleaned.parquet"
STAGING_ENRICHED = "/opt/airflow/data/staging/enriched_food_data.parquet"
MAX_OPENFOODFACTS_ROWS = int(os.environ.get("STAGING_MAX_OFF_ROWS", "300000"))


def _ensure_directories(paths: set[str]) -> None:
    for path in paths:
        if not path:
            continue
        os.makedirs(path, exist_ok=True)


_ensure_directories({
    os.path.dirname(LANDING_JSON),
    os.path.dirname(LANDING_PARQUET),
    os.path.dirname(STAGING_USDA),
    os.path.dirname(STAGING_OFF),
    os.path.dirname(STAGING_ENRICHED),
})


def clean_usda_data():
    """
    Lädt USDA JSON aus Landing Zone, bereinigt und normalisiert die Daten.
    """
    with open(LANDING_JSON, 'r') as f:
        data = json.load(f)
    
    # Extrahiere FoundationFoods
    foods = data.get('FoundationFoods', [])
    
    records = []
    for food in foods:
        food_id = food.get('fdcId')
        description = food.get('description')
        category = food.get('foodCategory', {}).get('description', 'Unknown')
        
        # Extrahiere Nährstoffe
        nutrients = {}
        for nutrient in food.get('foodNutrients', []):
            nutrient_name = nutrient.get('nutrient', {}).get('name')
            nutrient_amount = nutrient.get('amount')
            nutrient_unit = nutrient.get('nutrient', {}).get('unitName')
            
            if nutrient_name and nutrient_amount is not None:
                nutrients[nutrient_name] = {
                    'amount': nutrient_amount,
                    'unit': nutrient_unit
                }
        
        # Erstelle flache Struktur für wichtige Nährstoffe
        record = {
            'source': 'USDA',
            'food_id': food_id,
            'description': description,
            'category': category,
            'energy_kcal': nutrients.get('Energy', {}).get('amount'),
            'protein_g': nutrients.get('Protein', {}).get('amount'),
            'fat_g': nutrients.get('Total lipid (fat)', {}).get('amount'),
            'carbohydrate_g': nutrients.get('Carbohydrate, by difference', {}).get('amount'),
            'fiber_g': nutrients.get('Fiber, total dietary', {}).get('amount'),
            'sugar_g': nutrients.get('Sugars, total including NLEA', {}).get('amount'),
            'sodium_mg': nutrients.get('Sodium, Na', {}).get('amount'),
            'magnesium_mg': nutrients.get('Magnesium, Mg', {}).get('amount'),
            'vitamin_c_mg': nutrients.get('Vitamin C, total ascorbic acid', {}).get('amount'),
            'vitamin_a_ug': nutrients.get('Vitamin A, RAE', {}).get('amount'),
            'vitamin_d_ug': nutrients.get('Vitamin D (D2 + D3)', {}).get('amount'),
            'vitamin_e_mg': nutrients.get('Vitamin E (alpha-tocopherol)', {}).get('amount'),
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Datenbereinigung
    # 1. Entferne Duplikate basierend auf food_id
    df = df.drop_duplicates(subset=['food_id'], keep='first')
    
    # 2. Entferne Zeilen ohne Beschreibung
    df = df.dropna(subset=['description'])
    
    # 3. Fülle fehlende numerische Werte mit 0 (konservativ)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # 4. Normalisiere Kategorien (lowercase, strip)
    df['category'] = df['category'].str.lower().str.strip()
    df['description'] = df['description'].str.lower().str.strip()
    
    # Speichere in Staging
    df.to_parquet(STAGING_USDA, index=False)
    print(f"✅ USDA data cleaned: {len(df)} records saved to {STAGING_USDA}")


def clean_openfoodfacts_data():
    """
    Lädt OpenFoodFacts Parquet aus Landing Zone, bereinigt und normalisiert die Daten.
    Nutzt Chunking + Streaming-Write, um Memory-Probleme zu vermeiden.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    relevant_cols = [
        'code', 'product_name', 'brands', 'categories',
        'energy-kcal_100g', 'proteins_100g', 'fat_100g',
        'carbohydrates_100g', 'sugars_100g', 'fiber_100g',
        'salt_100g', 'sodium_100g', 'nutrition-score-fr_100g',
        'main_category', 'nutriments'
    ]

    rename_map = {
        'code': 'food_id',
        'product_name': 'description',
        'main_category': 'category',
        'energy-kcal_100g': 'energy_kcal',
        'proteins_100g': 'protein_g',
        'fat_100g': 'fat_g',
        'carbohydrates_100g': 'carbohydrate_g',
        'sugars_100g': 'sugar_g',
        'fiber_100g': 'fiber_g',
        'sodium_100g': 'sodium_mg',
        'salt_100g': 'salt_g'
    }

    def extract_nutrients(nutrient_list):
        wanted = {"energy-kcal", "proteins", "fat", "carbohydrates", "sugars", "fiber", "salt", "sodium"}
        result = {}

        if nutrient_list is None:
            nutrient_list = []
        for n in nutrient_list:
            name = n.get("name")
            if name in wanted:
                result[name] = n.get("100g")  # or "value"

        return result

    parquet_file = pq.ParquetFile(LANDING_PARQUET)
    available_columns = [col for col in relevant_cols if col in parquet_file.schema_arrow.names]
    chunk_size = 10000
    print(f"📊 Reading {parquet_file.metadata.num_rows} rows in chunks of {chunk_size:,}...")

    temp_output = f"{STAGING_OFF}.tmp"
    if os.path.exists(temp_output):
        os.remove(temp_output)

    writer = None
    total_rows = 0
    total_kept = 0

    for batch in parquet_file.iter_batches(batch_size=chunk_size, columns=available_columns):
        df_chunk = batch.to_pandas()
        total_rows += len(df_chunk)

        if 'product_name' in df_chunk.columns:
            def extract_product_name(arr):
                if isinstance(arr, (list, tuple)) and len(arr) > 0:
                    first_entry = arr[0]
                    if isinstance(first_entry, dict) and 'text' in first_entry:
                        return first_entry['text']
                return None

            df_chunk['product_name'] = df_chunk['product_name'].apply(extract_product_name)

        # Extract nutrients from nutriments column
        if 'nutriments' in df_chunk.columns:
            df_chunk['nutrients_extracted'] = df_chunk['nutriments'].apply(extract_nutrients)
            df_chunk['energy-kcal_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('energy-kcal'))
            df_chunk['proteins_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('proteins'))
            df_chunk['fat_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('fat'))
            df_chunk['carbohydrates_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('carbohydrates'))
            df_chunk['sugars_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('sugars'))
            df_chunk['fiber_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('fiber'))
            df_chunk['salt_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('salt'))
            df_chunk['sodium_100g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('sodium'))
            df_chunk = df_chunk.drop(columns=['nutriments', 'nutrients_extracted'])

        df_chunk = df_chunk.rename(columns=rename_map)
        df_chunk['source'] = 'OpenFoodFacts'

        if 'description' not in df_chunk.columns:
            df_chunk['description'] = None

        for fallback_col in ('brands', 'categories'):
            if fallback_col in df_chunk.columns:
                df_chunk['description'] = df_chunk['description'].fillna(df_chunk[fallback_col])

        df_chunk['description'] = df_chunk['description'].fillna('unknown product')

        numeric_cols = df_chunk.select_dtypes(include=['float64', 'int64']).columns
        df_chunk[numeric_cols] = df_chunk[numeric_cols].fillna(0)

        if 'category' in df_chunk.columns:
            df_chunk['category'] = df_chunk['category'].fillna('unknown').str.lower().str.strip()
        df_chunk['description'] = df_chunk['description'].str.lower().str.strip()

        df_chunk = df_chunk.drop_duplicates(subset=['food_id'], keep='first')

        if df_chunk.empty:
            continue

        total_kept += len(df_chunk)

        table = pa.Table.from_pandas(df_chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(temp_output, table.schema, compression="snappy")
        writer.write_table(table)

    if writer is not None:
        writer.close()
        os.replace(temp_output, STAGING_OFF)
    else:
        empty_df = pd.DataFrame(columns=list(rename_map.values()) + ['source'])
        empty_df.to_parquet(STAGING_OFF, index=False)

    print(
        f"✅ OpenFoodFacts data cleaned: {total_kept} records saved to {STAGING_OFF} "
        f"(processed {total_rows} rows)"
    )


def enrich_and_merge():
    """
    Lädt bereinigte USDA- und OpenFoodFacts-Daten, führt sie zusammen und erstellt
    einen angereicherten Datensatz für die Analyse.
    """
    try:
        print("📥 Loading USDA data...")
        usda = pd.read_parquet(STAGING_USDA)
        print(f"   USDA: {len(usda)} rows, {len(usda.columns)} columns")

        print("📥 Loading OpenFoodFacts data...")
        if MAX_OPENFOODFACTS_ROWS > 0:
            import pyarrow.parquet as pq

            parquet_off = pq.ParquetFile(STAGING_OFF)
            total_off = parquet_off.metadata.num_rows
            batches = []
            rows_needed = MAX_OPENFOODFACTS_ROWS

            for batch in parquet_off.iter_batches(batch_size=50000):
                df_batch = batch.to_pandas()
                batches.append(df_batch)
                rows_needed -= len(df_batch)
                if rows_needed <= 0:
                    break

            if batches:
                off = pd.concat(batches, ignore_index=True)
                if len(off) > MAX_OPENFOODFACTS_ROWS:
                    off = off.head(MAX_OPENFOODFACTS_ROWS)
            else:
                off = pd.DataFrame()

            print(
                f"   OpenFoodFacts: {len(off)} rows used (total {total_off}, limit {MAX_OPENFOODFACTS_ROWS})"
            )
        else:
            off = pd.read_parquet(STAGING_OFF)
            print(f"   OpenFoodFacts: {len(off)} rows, {len(off.columns)} columns")

        all_cols = set(usda.columns).union(set(off.columns))
        print(f"🔄 Harmonizing {len(all_cols)} unique columns...")

        for col in all_cols:
            if col not in usda.columns:
                usda[col] = None
            if col not in off.columns:
                off[col] = None

        usda = usda[sorted(usda.columns)]
        off = off[sorted(off.columns)]

        print("🔗 Merging datasets...")
        enriched = pd.concat([usda, off], ignore_index=True)
        print(f"   Combined: {len(enriched)} rows")

        print("🧮 Computing vitamin density...")
        vitamin_cols = ['vitamin_c_mg', 'vitamin_a_ug', 'vitamin_d_ug', 'vitamin_e_mg']
        available_vitamin_cols = [col for col in vitamin_cols if col in enriched.columns]

        if available_vitamin_cols:
            enriched['total_vitamins'] = enriched[available_vitamin_cols].fillna(0).sum(axis=1)
            enriched['vitamin_density'] = enriched.apply(
                lambda row: row['total_vitamins'] / row['energy_kcal'] if row.get('energy_kcal', 0) > 0 else 0,
                axis=1
            )
        else:
            enriched['total_vitamins'] = 0
            enriched['vitamin_density'] = 0

        print("🏷️  Categorizing food types...")
        enriched['food_type'] = enriched['source'].apply(
            lambda x: 'raw' if x == 'USDA' else 'processed'
        )

        print("🔄 Converting food_id to string for Parquet compatibility...")
        enriched['food_id'] = enriched['food_id'].astype(str)

        print(f"💾 Saving enriched dataset to {STAGING_ENRICHED}...")
        enriched.to_parquet(STAGING_ENRICHED, index=False)
        print(f"✅ Enriched dataset created: {len(enriched)} records saved to {STAGING_ENRICHED}")
        print(f"   - USDA (raw): {len(usda)} records")
        print(f"   - OpenFoodFacts (processed): {len(off)} records")

    except Exception as e:
        print(f"❌ ERROR in enrich_and_merge: {e}")
        import traceback
        traceback.print_exc()
        raise


with DAG(
    dag_id="staging_pipeline",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["staging", "cleaning", "transformation"],
) as dag:

    clean_usda = PythonOperator(
        task_id="clean_usda_data",
        python_callable=clean_usda_data,
    )

    clean_off = PythonOperator(
        task_id="clean_openfoodfacts_data",
        python_callable=clean_openfoodfacts_data,
    )

    enrich = PythonOperator(
        task_id="enrich_and_merge",
        python_callable=enrich_and_merge,
    )

    # Pipeline: Bereinige beide Datasets parallel, dann merge
    [clean_usda, clean_off] >> enrich
