import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

# Paths
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
    Loads USDA JSON from Landing Zone, cleans and normalizes the data.
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
        
        # Extract nutrients
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
        
        # Create flat structure for important nutrients
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
    
    # Data cleaning
    # 1. Remove duplicates based on food_id
    df = df.drop_duplicates(subset=['food_id'], keep='first')
    
    # 2. Remove rows without description
    df = df.dropna(subset=['description'])
    
    # 3. Fill missing numeric values with 0 (conservative)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # 4. Normalize categories (lowercase, strip)
    df['category'] = df['category'].str.lower().str.strip()
    df['description'] = df['description'].str.lower().str.strip()
    
    # Save to Staging
    df.to_parquet(STAGING_USDA, index=False)
    print(f"✅ USDA data cleaned: {len(df)} records saved to {STAGING_USDA}")


def clean_openfoodfacts_data():
    """
    Loads OpenFoodFacts Parquet from Landing Zone, cleans and normalizes the data.
    Uses chunking + streaming write to avoid memory issues.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    relevant_cols = [
        'code', 'product_name', 'brands', 'categories_tags',
        'energy-kcal_100g', 'proteins_100g', 'fat_100g',
        'carbohydrates_100g', 'sugars_100g', 'fiber_100g',
        'salt_100g', 'sodium_100g', 'nutrition-score-fr_100g',
        'main_category', 'nutriments'
    ]

    rename_map = {
        'code': 'food_id',
        'product_name': 'description',
        'main_category': 'category'
    }

    # Mapping from OpenFoodFacts categories to USDA category names
    category_mapping = {
        # Sweets and snacks
        'en:snacks': 'sweets',
        'en:sweet-snacks': 'sweets',
        'en:cocoa-and-its-products': 'sweets',
        'en:confectioneries': 'sweets',
        'en:chocolates': 'sweets',
        'en:candies': 'sweets',
        'en:biscuits-and-cakes': 'baked products',
        'en:cakes': 'baked products',
        'en:biscuits': 'baked products',
        'en:cookies': 'baked products',
        'en:ice-creams': 'sweets',
        'en:desserts': 'sweets',
        'en:pies': 'baked products',
        'en:pastries': 'baked products',
        'en:muffins': 'baked products',
        'en:donuts': 'baked products',

        # Fruits and vegetables
        'en:plant-based-foods': 'vegetables and vegetable products',
        'en:plant-based-foods-and-beverages': 'vegetables and vegetable products',
        'en:cereals-and-potatoes': 'cereal grains and pasta',
        'en:fruits-and-vegetables-based-foods': 'fruits and fruit juices',
        'en:fruits': 'fruits and fruit juices',
        'en:vegetables': 'vegetables and vegetable products',
        'en:apples': 'fruits and fruit juices',
        'en:bananas': 'fruits and fruit juices',
        'en:oranges': 'fruits and fruit juices',
        'en:berries': 'fruits and fruit juices',
        'en:citrus': 'fruits and fruit juices',
        'en:tropical-fruits': 'fruits and fruit juices',
        'en:potatoes': 'vegetables and vegetable products',
        'en:carrots': 'vegetables and vegetable products',
        'en:tomatoes': 'vegetables and vegetable products',
        'en:onions': 'vegetables and vegetable products',
        'en:lettuce': 'vegetables and vegetable products',
        'en:spinach': 'vegetables and vegetable products',
        'en:broccoli': 'vegetables and vegetable products',
        'en:peppers': 'vegetables and vegetable products',

        # Beverages
        'en:beverages': 'beverages',
        'en:carbonated-drinks': 'beverages',
        'en:sodas': 'beverages',
        'en:juices': 'beverages',
        'en:fruit-juices': 'beverages',
        'en:vegetable-juices': 'beverages',
        'en:waters': 'beverages',
        'en:teas': 'beverages',
        'en:coffees': 'beverages',
        'en:milks': 'beverages',
        'en:smoothies': 'beverages',
        'en:energy-drinks': 'beverages',

        # Meats and proteins
        'en:meats': 'beef products',
        'en:pork': 'pork products',
        'en:poultry': 'poultry products',
        'en:chicken': 'poultry products',
        'en:turkey': 'poultry products',
        'en:beef': 'beef products',
        'en:lamb': 'lamb, veal, and game products',
        'en:veal': 'lamb, veal, and game products',
        'en:game': 'lamb, veal, and game products',
        'en:fish': 'finfish and shellfish products',
        'en:seafood': 'finfish and shellfish products',
        'en:salmon': 'finfish and shellfish products',
        'en:tuna': 'finfish and shellfish products',
        'en:shrimp': 'finfish and shellfish products',
        'en:eggs': 'dairy and egg products',
        'en:sausages': 'sausages and luncheon meats',
        'en:luncheon-meats': 'sausages and luncheon meats',
        'en:hams': 'sausages and luncheon meats',
        'en:bacons': 'sausages and luncheon meats',

        # Dairy
        'en:dairy': 'dairy and egg products',
        'en:cheeses': 'dairy and egg products',
        'en:yogurts': 'dairy and egg products',
        'en:butters': 'dairy and egg products',
        'en:creams': 'dairy and egg products',
        'en:milks': 'dairy and egg products',

        # Grains and pasta
        'en:pastas': 'cereal grains and pasta',
        'en:noodles': 'cereal grains and pasta',
        'en:rices': 'cereal grains and pasta',
        'en:breads': 'baked products',
        'en:cereals': 'cereal grains and pasta',
        'en:oats': 'cereal grains and pasta',
        'en:wheats': 'cereal grains and pasta',

        # Nuts and seeds
        'en:nuts': 'nut and seed products',
        'en:seeds': 'nut and seed products',
        'en:almonds': 'nut and seed products',
        'en:walnuts': 'nut and seed products',
        'en:peanuts': 'nut and seed products',
        'en:sunflower-seeds': 'nut and seed products',
        'en:pumpkin-seeds': 'nut and seed products',

        # Legumes
        'en:legumes': 'legumes and legume products',
        'en:beans': 'legumes and legume products',
        'en:lentils': 'legumes and legume products',
        'en:peas': 'legumes and legume products',
        'en:chickpeas': 'legumes and legume products',

        # Fats and oils
        'en:fats': 'fats and oils',
        'en:oils': 'fats and oils',
        'en:olive-oils': 'fats and oils',
        'en:sunflower-oils': 'fats and oils',
        'en:coconut-oils': 'fats and oils',
        'en:butters': 'fats and oils',

        # Soups and sauces
        'en:soups': 'soups, sauces, and gravies',
        'en:sauces': 'soups, sauces, and gravies',
        'en:gravies': 'soups, sauces, and gravies',
        'en:salad-dressings': 'soups, sauces, and gravies',
        'en:ketchups': 'soups, sauces, and gravies',
        'en:mustards': 'soups, sauces, and gravies',

        # Spices and herbs
        'en:spices': 'spices and herbs',
        'en:herbs': 'spices and herbs',
        'en:peppers': 'spices and herbs',
        'en:salts': 'spices and herbs',
        'en:sugars': 'spices and herbs',

        # Restaurant foods
        'en:restaurant-foods': 'restaurant foods',
        'en:fast-foods': 'restaurant foods',
        'en:pizzas': 'restaurant foods',
        'en:burgers': 'restaurant foods',
        'en:sandwiches': 'restaurant foods',

        # Add more as needed
    }

    def extract_nutrients(nutrient_list):
        wanted = {"energy-kcal", "proteins", "fat", "carbohydrates", "sugars", "fiber", "salt", "sodium", "iron", "calcium", "magnesium", "potassium", "zinc", "vitamin-a", "vitamin-c", "vitamin-d", "vitamin-e"}
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

        # Map categories_tags to USDA categories
        if 'categories_tags' in df_chunk.columns:
            def map_categories(categories):
                import numpy as np
                if not isinstance(categories, (list, np.ndarray)):
                    return None
                for cat in categories:
                    mapped = category_mapping.get(cat)
                    if mapped:
                        return mapped
                return None

            df_chunk['mapped_category'] = df_chunk['categories_tags'].apply(map_categories)

        # Extract nutrients from nutriments column
        if 'nutriments' in df_chunk.columns:
            df_chunk['nutrients_extracted'] = df_chunk['nutriments'].apply(extract_nutrients)
            df_chunk['energy_kcal'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('energy-kcal'))
            df_chunk['protein_g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('proteins'))
            df_chunk['fat_g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('fat'))
            df_chunk['carbohydrate_g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('carbohydrates'))
            df_chunk['sugar_g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('sugars'))
            df_chunk['fiber_g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('fiber'))
            df_chunk['salt_g'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('salt'))
            df_chunk['sodium_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('sodium'))
            df_chunk['iron_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('iron'))
            df_chunk['calcium_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('calcium'))
            df_chunk['magnesium_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('magnesium'))
            df_chunk['potassium_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('potassium'))
            df_chunk['zinc_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('zinc'))
            df_chunk['vitamin_a_ug'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('vitamin-a'))
            df_chunk['vitamin_c_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('vitamin-c'))
            df_chunk['vitamin_d_ug'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('vitamin-d'))
            df_chunk['vitamin_e_mg'] = df_chunk['nutrients_extracted'].apply(lambda x: x.get('vitamin-e'))
            df_chunk = df_chunk.drop(columns=['nutriments', 'nutrients_extracted'])

        # Ensure nutrient columns are numeric and fill nulls
        nutrient_cols = ['energy_kcal', 'protein_g', 'fat_g', 'carbohydrate_g', 'sugar_g', 'fiber_g', 'salt_g', 'sodium_mg', 'iron_mg', 'calcium_mg', 'magnesium_mg', 'potassium_mg', 'zinc_mg', 'vitamin_a_ug', 'vitamin_c_mg', 'vitamin_d_ug', 'vitamin_e_mg']
        for col in nutrient_cols:
            if col in df_chunk.columns:
                df_chunk[col] = pd.to_numeric(df_chunk[col], errors='coerce').fillna(0)

        # Convert g to mg for nutrients that are in g
        g_to_mg_cols = ['sodium_mg', 'iron_mg', 'calcium_mg', 'magnesium_mg', 'potassium_mg', 'zinc_mg', 'vitamin_c_mg', 'vitamin_e_mg']
        for col in g_to_mg_cols:
            if col in df_chunk.columns:
                df_chunk[col] = df_chunk[col] * 1000

        df_chunk = df_chunk.rename(columns=rename_map)
        df_chunk['source'] = 'OpenFoodFacts'

        # Ensure category column exists
        if 'category' not in df_chunk.columns:
            df_chunk['category'] = None

        # Use mapped category if available
        if 'mapped_category' in df_chunk.columns:
            df_chunk['category'] = df_chunk['mapped_category'].fillna(df_chunk['category'])
            df_chunk = df_chunk.drop(columns=['mapped_category'])

        if 'description' not in df_chunk.columns:
            df_chunk['description'] = None

        for fallback_col in ('brands', 'categories_tags'):
            if fallback_col in df_chunk.columns:
                df_chunk['description'] = df_chunk['description'].fillna(df_chunk[fallback_col])

        df_chunk['description'] = df_chunk['description'].fillna('unknown product').astype(str)

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
    Loads cleaned USDA and OpenFoodFacts data, merges them and creates
    an enriched dataset for analysis.
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

    # Pipeline: Clean both datasets in parallel, then merge
    [clean_usda, clean_off] >> enrich
