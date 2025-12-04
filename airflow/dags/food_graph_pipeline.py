from __future__ import annotations
import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from neo4j import GraphDatabase
import redis


def load_food_data_to_neo4j():
    """Load food data from parquet and create graph in Neo4j"""
    
    # Neo4j connection
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "neo4j")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # Load data
    df = pd.read_parquet("/opt/airflow/data/production/enriched_food_data_latest.parquet")
    print(f"Loaded {len(df)} food items")
    
    # Sample 100 items to keep graph manageable
    df_sample = df.head(100).copy()
    
    with driver.session() as session:
        # Clear existing data
        print("Clearing existing data...")
        session.run("MATCH (n) DETACH DELETE n")
        
        # Create constraints and indexes
        print("Creating constraints...")
        session.run("CREATE CONSTRAINT food_id IF NOT EXISTS FOR (f:Food) REQUIRE f.food_id IS UNIQUE")
        session.run("CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE")
        session.run("CREATE CONSTRAINT brand_name IF NOT EXISTS FOR (b:Brand) REQUIRE b.name IS UNIQUE")
        session.run("CREATE CONSTRAINT source_name IF NOT EXISTS FOR (s:Source) REQUIRE s.name IS UNIQUE")
        
        # Create Food nodes
        print("Creating Food nodes...")
        for _, row in df_sample.iterrows():
            session.run("""
                CREATE (f:Food {
                    food_id: $food_id,
                    description: $description,
                    energy_kcal: $energy_kcal,
                    protein_g: $protein_g,
                    carbohydrate_g: $carbohydrate_g,
                    fat_g: $fat_g,
                    fiber_g: $fiber_g,
                    sugar_g: $sugar_g,
                    total_vitamins: $total_vitamins,
                    vitamin_density: $vitamin_density,
                    food_type: $food_type
                })
            """, 
                food_id=str(row.get('food_id', '')),
                description=str(row.get('description', ''))[:200],  # Limit length
                energy_kcal=float(row['energy_kcal']) if pd.notna(row.get('energy_kcal')) else None,
                protein_g=float(row['protein_g']) if pd.notna(row.get('protein_g')) else None,
                carbohydrate_g=float(row['carbohydrate_g']) if pd.notna(row.get('carbohydrate_g')) else None,
                fat_g=float(row['fat_g']) if pd.notna(row.get('fat_g')) else None,
                fiber_g=float(row['fiber_g']) if pd.notna(row.get('fiber_g')) else None,
                sugar_g=float(row['sugar_g']) if pd.notna(row.get('sugar_g')) else None,
                total_vitamins=float(row['total_vitamins']) if pd.notna(row.get('total_vitamins')) else None,
                vitamin_density=float(row['vitamin_density']) if pd.notna(row.get('vitamin_density')) else None,
                food_type=str(row.get('food_type', '')) if pd.notna(row.get('food_type')) else None
            )
        
        # Create Category nodes and relationships
        print("Creating Categories and relationships...")
        categories = df_sample['category'].dropna().unique()
        for cat in categories:
            if cat and str(cat).strip():
                session.run("MERGE (c:Category {name: $name})", name=str(cat))
        
        for _, row in df_sample.iterrows():
            if pd.notna(row.get('category')):
                session.run("""
                    MATCH (f:Food {food_id: $food_id})
                    MATCH (c:Category {name: $category})
                    MERGE (f)-[:IN_CATEGORY]->(c)
                """, food_id=str(row['food_id']), category=str(row['category']))
        
        # Create Brand nodes and relationships
        print("Creating Brands...")
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
        
        # Create Source nodes and relationships
        print("Creating Sources...")
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
    
    # Neo4j connection
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "neo4j")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    with driver.session() as session:
        # Get category stats
        result = session.run("""
            MATCH (f:Food)-[:IN_CATEGORY]->(c:Category)
            WITH c.name as category, 
                 count(f) as food_count,
                 avg(f.energy_kcal) as avg_energy,
                 avg(f.protein_g) as avg_protein,
                 avg(f.vitamin_density) as avg_vitamin_density
            RETURN category, food_count, avg_energy, avg_protein, avg_vitamin_density
            ORDER BY food_count DESC
        """)
        
        print("Caching category statistics to Redis...")
        for record in result:
            cat = record['category']
            key_prefix = f"category:{cat}"
            
            # Cache with 1 hour TTL
            r.set(f"{key_prefix}:count", record['food_count'], ex=3600)
            if record['avg_energy']:
                r.set(f"{key_prefix}:avg_energy_kcal", round(record['avg_energy'], 2), ex=3600)
            if record['avg_protein']:
                r.set(f"{key_prefix}:avg_protein_g", round(record['avg_protein'], 2), ex=3600)
            if record['avg_vitamin_density']:
                r.set(f"{key_prefix}:avg_vitamin_density", round(record['avg_vitamin_density'], 4), ex=3600)
            
            print(f"  {cat}: {record['food_count']} foods")
    
    driver.close()
    print(f"Cached statistics for categories in Redis")


def validate_graph():
    """Validate the created graph structure"""
    
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "neo4j")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    with driver.session() as session:
        # Sample queries for your 5 questions
        print("\n=== Validation Queries ===")
        
        # 1. Top categories by food count
        print("\n1. Top 5 Categories:")
        result = session.run("""
            MATCH (f:Food)-[:IN_CATEGORY]->(c:Category)
            RETURN c.name as category, count(f) as count
            ORDER BY count DESC LIMIT 5
        """)
        for rec in result:
            print(f"   {rec['category']}: {rec['count']} foods")
        
        # 2. High vitamin density foods
        print("\n2. Top 5 High Vitamin Density Foods:")
        result = session.run("""
            MATCH (f:Food)
            WHERE f.vitamin_density IS NOT NULL
            RETURN f.description as food, f.vitamin_density as density
            ORDER BY density DESC LIMIT 5
        """)
        for rec in result:
            print(f"   {rec['food'][:50]}: {rec['density']:.4f}")
        
        # 3. Brands with most products
        print("\n3. Brands with Most Products:")
        result = session.run("""
            MATCH (f:Food)-[:MANUFACTURED_BY]->(b:Brand)
            RETURN b.name as brand, count(f) as count
            ORDER BY count DESC LIMIT 5
        """)
        for rec in result:
            print(f"   {rec['brand']}: {rec['count']} products")
        
        # 4. Source comparison
        print("\n4. Foods by Source:")
        result = session.run("""
            MATCH (f:Food)-[:FROM_SOURCE]->(s:Source)
            RETURN s.name as source, count(f) as count
        """)
        for rec in result:
            print(f"   {rec['source']}: {rec['count']} foods")
    
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
