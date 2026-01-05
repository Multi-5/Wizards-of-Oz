import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Sample data mimicking OpenFoodFacts structure
sample_data = {
    'code': ['12345', '67890'],
    'product_name': ['Chocolate Bar', 'Apple'],
    'brands': ['BrandA', 'BrandB'],
    'categories_tags': [['en:snacks', 'en:sweet-snacks', 'en:cocoa-and-its-products', 'en:confectioneries'], ['en:fruits', 'en:apples']],
    'nutriments': [
        [
            {'name': 'energy-kcal', '100g': 617.0},
            {'name': 'proteins', '100g': 8.0},
            {'name': 'fat', '100g': 48.0},
            {'name': 'carbohydrates', '100g': 36.0},
            {'name': 'sugars', '100g': 32.0},
            {'name': 'salt', '100g': 0.01},
            {'name': 'sodium', '100g': 0.004}
        ],
        [
            {'name': 'energy-kcal', '100g': 52.0},
            {'name': 'proteins', '100g': 0.2},
            {'name': 'fat', '100g': 0.2},
            {'name': 'carbohydrates', '100g': 14.0},
            {'name': 'sugars', '100g': 10.0},
            {'name': 'fiber', '100g': 2.4},
            {'name': 'salt', '100g': 0.0},
            {'name': 'sodium', '100g': 0.0}
        ]
    ],
    'nutrition-score-fr_100g': [25.0, 0.0],
    'main_category': ['en:chocolates', 'en:apples']
}

df = pd.DataFrame(sample_data)
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data/landing/food.parquet')

print("Sample food.parquet created.")