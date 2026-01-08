from __future__ import annotations
import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from neo4j import GraphDatabase
import redis
import json

DATA_FILE = "/opt/airflow/data/staging/enriched_food_data.parquet"
MAX_RECORDS_PER_SOURCE = int(os.environ.get("GRAPH_SAMPLE_PER_SOURCE", "50"))
MAX_TOTAL_RECORDS = int(os.environ.get("GRAPH_SAMPLE_TOTAL", "200"))


def _ensure_directories(paths: set[str]) -> None:
    for path in paths:
        if not path:
            continue
        os.makedirs(path, exist_ok=True)


_ensure_directories({os.path.dirname(DATA_FILE)})


def _select_balanced_sample(df: pd.DataFrame) -> pd.DataFrame:
    """Return a sample ensuring each source is represented."""
    if df.empty:
        return df
    if 'source' not in df.columns:
        return df.head(MAX_TOTAL_RECORDS).copy()

    samples: list[pd.DataFrame] = []
    for source, group in df.groupby('source'):
        if group.empty:
            continue
        samples.append(group.head(MAX_RECORDS_PER_SOURCE))

    if not samples:
        return df.head(MAX_TOTAL_RECORDS).copy()

    sample_df = pd.concat(samples, ignore_index=True)
    if MAX_TOTAL_RECORDS:
        sample_df = sample_df.head(MAX_TOTAL_RECORDS)
    return sample_df


def load_food_data_to_neo4j():
    """Load food data from parquet and create graph in Neo4j"""
    
    # Neo4j connection
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "neo4j")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # Load data
    df = pd.read_parquet(DATA_FILE)
    print(f"Loaded {len(df)} food items")
    
    # Sample records while keeping both USDA and OpenFoodFacts represented
    df_sample = _select_balanced_sample(df)
    print(
        "Sampling graph data",
        {src: len(group) for src, group in df_sample.groupby('source', dropna=False)}
    )
    
    with driver.session() as session:
        # Clear existing data
        print("Clearing existing data...")
        session.run("MATCH (n) DETACH DELETE n")

        # Create constraints and indexes
        print("Creating constraints...")
        session.run("CREATE CONSTRAINT food_id IF NOT EXISTS FOR (f:Food) REQUIRE f.food_id IS UNIQUE")
        session.run("CREATE CONSTRAINT brand_name IF NOT EXISTS FOR (b:Brand) REQUIRE b.name IS UNIQUE")
        session.run("CREATE CONSTRAINT source_name IF NOT EXISTS FOR (s:Source) REQUIRE s.name IS UNIQUE")
        session.run("CREATE CONSTRAINT nutrient_name IF NOT EXISTS FOR (n:Nutrient) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE")

        def _extract_product_name(val):
            """Extract product name text from OpenFoodFacts product_name field.
            Accepts list/dict or JSON string. Prefers lang=='main' or 'en'."""
            if val is None:
                return None
            try:
                # If it's a JSON string, parse it
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                    except Exception:
                        # not a JSON string, return raw
                        return val
                else:
                    parsed = val

                # parsed expected to be a list of dicts or a dict
                if isinstance(parsed, dict):
                    # maybe single object with text
                    if 'text' in parsed:
                        return parsed.get('text')
                    return None
                if isinstance(parsed, list):
                    # prefer main or en
                    for lang in ('main', 'en'):
                        for item in parsed:
                            if isinstance(item, dict) and item.get('lang') == lang and item.get('text'):
                                return item.get('text')
                    # fallback to first text value
                    for item in parsed:
                        if isinstance(item, dict) and item.get('text'):
                            return item.get('text')
            except Exception:
                return None
            return None

        # Create Food/Product nodes (keep meta fields only, nutrients will be separate nodes)
        print("Creating Food nodes (metadata only)...")
        for _, row in df_sample.iterrows():
            # Prefer product_name for OpenFoodFacts if available
            description = None
            if pd.notna(row.get('description')):
                description = str(row.get('description'))[:200]
            if str(row.get('source', '')).lower() == 'openfoodfacts' and pd.notna(row.get('product_name')):
                pn = _extract_product_name(row.get('product_name'))
                if pn:
                    description = str(pn)[:200]

            session.run(
                """
                CREATE (f:Food {
                    food_id: $food_id,
                    description: $description,
                    food_type: $food_type,
                    category: $category
                })
                """,
                food_id=str(row.get('food_id', '')),
                description=(description if description is not None else ''),
                food_type=(str(row.get('food_type', '')) if pd.notna(row.get('food_type')) else None),
                category=(str(row.get('category')) if pd.notna(row.get('category')) else ''),
            )

        # Create Brand nodes and relationships (unchanged)
        print("Creating Brands and relationships...")
        if 'brands' in df_sample.columns:
            brands = df_sample['brands'].dropna().unique()
            for brand in brands:
                if brand and str(brand).strip():
                    session.run("MERGE (b:Brand {name: $name})", name=str(brand))

            for _, row in df_sample.iterrows():
                if pd.notna(row.get('brands')):
                    session.run("""
                        MATCH (f:Food {food_id: $food_id})
                        MATCH (b:Brand {name: $brand})
                        MERGE (f)-[:MANUFACTURED_BY]->(b)
                    """, food_id=str(row['food_id']), brand=str(row['brands']))

        # Create Source nodes and relationships (unchanged)
        print("Creating Sources and relationships...")
        if 'source' in df_sample.columns:
            sources = df_sample['source'].dropna().unique()
            for source in sources:
                if source and str(source).strip():
                    session.run("MERGE (s:Source {name: $name})", name=str(source))

            for _, row in df_sample.iterrows():
                if pd.notna(row.get('source')):
                    session.run("""
                        MATCH (f:Food {food_id: $food_id})
                        MATCH (s:Source {name: $source})
                        MERGE (f)-[:FROM_SOURCE]->(s)
                    """, food_id=str(row['food_id']), source=str(row['source']))

        # Create Category nodes and IN_CATEGORY relationships
        print("Creating Category nodes and IN_CATEGORY relationships...")
        if 'category' in df_sample.columns:
            categories = [c for c in df_sample['category'].dropna().unique() if c and str(c).strip()]
            for cat in categories:
                session.run("MERGE (c:Category {name: $name})", name=str(cat))

            for _, row in df_sample.iterrows():
                if pd.notna(row.get('category')) and str(row.get('category')).strip():
                    session.run("""
                        MATCH (f:Food {food_id: $food_id})
                        MATCH (c:Category {name: $category})
                        MERGE (f)-[:IN_CATEGORY]->(c)
                    """, food_id=str(row['food_id']), category=str(row['category']))

        # Identify nutrient columns (numeric columns excluding identifiers and meta)
        meta_cols = {"food_id", "description", "category", "brands", "source", "food_type"}
        nutrient_cols = [c for c in df_sample.columns if c not in meta_cols and pd.api.types.is_numeric_dtype(df_sample[c])]
        print(f"Identified nutrient columns: {nutrient_cols}")

        # Create Nutrient nodes and HAS_NUTRIENT relationships
        print("Creating Nutrient nodes and HAS_NUTRIENT relationships...")
        for nutrient in nutrient_cols:
            if not nutrient or not str(nutrient).strip():
                continue
            # Infer unit simply from column name (heuristic)
            unit = None
            n_lower = nutrient.lower()
            if n_lower.endswith("_g") or n_lower in ("protein_g", "fat_g", "carbohydrate_g", "sugar_g", "fiber_g"):
                unit = "g"
            elif "kcal" in n_lower or "energy" in n_lower:
                unit = "kcal"
            elif n_lower.endswith("_mg") or "mg" in n_lower:
                unit = "mg"

            # Neo4j will reject null property values in a MERGE map; avoid passing unit when unknown
            if unit is None:
                session.run("MERGE (n:Nutrient {name: $name})", name=str(nutrient))
            else:
                session.run("MERGE (n:Nutrient {name: $name}) SET n.unit = $unit", name=str(nutrient), unit=unit)

        # Create relationships with amount property
        for _, row in df_sample.iterrows():
            fid = str(row.get('food_id', ''))
            for nutrient in nutrient_cols:
                val = row.get(nutrient)
                if pd.isna(val):
                    continue
                try:
                    amount = float(val)
                except Exception:
                    continue
                session.run(
                    """
                    MATCH (f:Food {food_id: $food_id})
                    MATCH (n:Nutrient {name: $nutrient})
                    MERGE (f)-[r:HAS_NUTRIENT]->(n)
                    SET r.amount = $amount
                    """,
                    food_id=fid,
                    nutrient=str(nutrient),
                    amount=amount,
                )
        
        # Get stats
        result = session.run("""
            MATCH (f:Food)
            OPTIONAL MATCH (c:Category)
            OPTIONAL MATCH (b:Brand)
            OPTIONAL MATCH (s:Source)
            OPTIONAL MATCH ()-[r]->()
            RETURN count(DISTINCT f) as foods, 
                   count(DISTINCT c) as categories, 
                   count(DISTINCT b) as brands,
                   count(DISTINCT s) as sources,
                   count(r) as relationships
        """)
        stats = result.single()
        print(f"Graph created: {stats['foods']} foods, {stats['categories']} categories, "
              f"{stats['brands']} brands, {stats['sources']} sources, {stats['relationships']} relationships")
    
    driver.close()


def cache_category_stats_to_redis():
    """Calculate category statistics and cache in Redis"""
    # Redis connection
    host = os.environ.get("REDIS_HOST", "redis")
    port = int(os.environ.get("REDIS_PORT", 6379))
    db = int(os.environ.get("REDIS_DB", 0))
    r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    # Read the parquet file directly and compute category stats instead of querying Neo4j
    if not os.path.exists(DATA_FILE):
        print(f"Data file not found: {DATA_FILE}")
        return

    df = pd.read_parquet(DATA_FILE)
    if 'category' not in df.columns:
        print("No category column in data; nothing to cache")
        return

    print("Computing category statistics from Parquet and caching to Redis...")
    grouped = df.groupby('category')
    for cat, group in grouped:
        if pd.isna(cat):
            continue
        key_prefix = f"category:{cat}"
        food_count = int(len(group))
        avg_energy = float(group['energy_kcal'].dropna().mean()) if 'energy_kcal' in group.columns and not group['energy_kcal'].dropna().empty else None
        avg_protein = float(group['protein_g'].dropna().mean()) if 'protein_g' in group.columns and not group['protein_g'].dropna().empty else None
        # vitamin_density may not exist; compute if available
        avg_vitamin_density = float(group['vitamin_density'].dropna().mean()) if 'vitamin_density' in group.columns and not group['vitamin_density'].dropna().empty else None

        # Cache with 1 hour TTL
        r.set(f"{key_prefix}:count", food_count, ex=3600)
        if avg_energy is not None:
            r.set(f"{key_prefix}:avg_energy_kcal", round(avg_energy, 2), ex=3600)
        if avg_protein is not None:
            r.set(f"{key_prefix}:avg_protein_g", round(avg_protein, 2), ex=3600)
        if avg_vitamin_density is not None:
            r.set(f"{key_prefix}:avg_vitamin_density", round(avg_vitamin_density, 4), ex=3600)

        print(f"  {cat}: {food_count} foods")

    print(f"Cached statistics for categories in Redis")


def validate_graph():
    """Validate the created graph structure"""
    
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "neo4j")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    with driver.session() as session:
        print("\n=== Validation Queries (nutrient graph) ===")

        # 1) Highest-Protein Foods Across Brands and Sources
        print("\n1) Highest-Protein Foods Across Brands and Sources:")
        q1 = """
        MATCH (f:Food)-[r:HAS_NUTRIENT]->(n:Nutrient {name:'protein_g'})
        OPTIONAL MATCH (f)-[:MANUFACTURED_BY]->(b:Brand)
        OPTIONAL MATCH (f)-[:FROM_SOURCE]->(s:Source)
        WITH f, r, n, b, s
        ORDER BY r.amount DESC
        LIMIT 50
        RETURN f, r, n, b, s
        """
        result = session.run(q1)
        for rec in result:
            f = rec.get('f')
            r = rec.get('r')
            n = rec.get('n')
            b = rec.get('b')
            s = rec.get('s')
            f_props = dict(f) if f is not None else {}
            n_props = dict(n) if n is not None else {}
            r_props = dict(r) if r is not None else {}
            b_props = dict(b) if b is not None else {}
            s_props = dict(s) if s is not None else {}
            print(f"  - {f_props.get('food_id','<no-id>')} | {f_props.get('description','')[:60]} | protein={r_props.get('amount')} | brand={b_props.get('name')} | source={s_props.get('name')}")

        # 2) Foods with High Sodium or Salt Content (Raw & Processed)
        print("\n2) Foods with High Sodium or Salt Content (Raw & Processed):")
        q2 = """
        MATCH (f:Food)-[r:HAS_NUTRIENT]->(n:Nutrient)
        WHERE toLower(coalesce(f.food_type,'')) IN ['raw','processed']
        AND (toLower(n.name) CONTAINS 'sodium' OR toLower(n.name) CONTAINS 'salt')
        AND (
          (toLower(coalesce(n.unit,'')) = 'mg' AND r.amount > 20)
          OR (toLower(coalesce(n.unit,'')) = 'g' AND r.amount > 0.02)
          OR (n.unit IS NULL AND r.amount > 20)
        )
        RETURN f, r, n
        LIMIT 200
        """
        result = session.run(q2)
        for rec in result:
            f = rec.get('f')
            r = rec.get('r')
            n = rec.get('n')
            f_props = dict(f) if f is not None else {}
            n_props = dict(n) if n is not None else {}
            r_props = dict(r) if r is not None else {}
            print(f"  - {f_props.get('food_id','<no-id>')} | {f_props.get('description','')[:60]} | nutrient={n_props.get('name')} ({n_props.get('unit')}) amount={r_props.get('amount')}")

        # 3) Foods in the Fats and Oils Category: Nutrient Breakdown by Source
        print("\n3) Foods in the Fats and Oils Category: Nutrient Breakdown by Source:")
        q3 = """
        MATCH (c:Category {name: "fats and oils"})
        MATCH (s:Source)<-[:FROM_SOURCE]-(f:Food)-[:IN_CATEGORY]->(c)
        MATCH (f)-[r:HAS_NUTRIENT]->(n:Nutrient)
        WHERE n.name IN [
        'energy_kcal','protein_g','fat_g','carbohydrate_g','sugar_g','fiber_g',
        'vitamin_c_mg','magnesium_mg'
        ]
        AND r.amount > 0
        RETURN s, c, f, r, n
        LIMIT 200
        """
        result = session.run(q3)
        for rec in result:
            s = rec.get('s')
            c = rec.get('c')
            f = rec.get('f')
            r = rec.get('r')
            n = rec.get('n')
            s_props = dict(s) if s is not None else {}
            c_props = dict(c) if c is not None else {}
            f_props = dict(f) if f is not None else {}
            r_props = dict(r) if r is not None else {}
            n_props = dict(n) if n is not None else {}
            print(f"  - category={c_props.get('name')} | source={s_props.get('name')} | {f_props.get('food_id','<no-id>')} | {f_props.get('description','')[:60]} | {n_props.get('name')}={r_props.get('amount')}")

        # 4) Nutrient-Level Average Comparison Between OpenFoodFacts and USDA
        print("\n4) Nutrient-Level Average Comparison Between OpenFoodFacts and USDA:")
        q4 = """
        MATCH (n:Nutrient)
        CALL {
          WITH n
          OPTIONAL MATCH (s1:Source {name:'USDA'})<-[:FROM_SOURCE]-(fu:Food)-[r1:HAS_NUTRIENT]->(n)
          RETURN avg(r1.amount) AS avg_usda
        }
        CALL {
          WITH n
          OPTIONAL MATCH (s2:Source {name:'OpenFoodFacts'})<-[:FROM_SOURCE]-(fo:Food)-[r2:HAS_NUTRIENT]->(n)
          RETURN avg(r2.amount) AS avg_off
        }
        RETURN n.name AS nutrient,
               round(coalesce(avg_usda,0), 6) AS avg_usda,
               round(coalesce(avg_off,0), 6) AS avg_openfoodfacts,
               round(coalesce(avg_off,0) - coalesce(avg_usda,0), 6) AS diff
        ORDER BY abs(diff) DESC
        LIMIT 50
        """
        result = session.run(q4)
        for rec in result:
            print(f"  - {rec['nutrient']}: avg_usda={rec['avg_usda']}, avg_off={rec['avg_openfoodfacts']}, diff={rec['diff']}")
    
    driver.close()


with DAG(
    dag_id="food_graph_pipeline",
    description="Load food data into Neo4j graph and cache stats in Redis",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["neo4j", "redis", "food"],
) as dag:
    
    load_task = PythonOperator(
        task_id="load_food_graph",
        python_callable=load_food_data_to_neo4j,
    )
    
    cache_task = PythonOperator(
        task_id="cache_category_stats",
        python_callable=cache_category_stats_to_redis,
    )
    
    validate_task = PythonOperator(
        task_id="validate_graph",
        python_callable=validate_graph,
    )
    
    load_task >> cache_task >> validate_task
