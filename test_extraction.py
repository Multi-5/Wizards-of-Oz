import pandas as pd

# Simulate the nutriments data from the example
nutriments_example = [
    {'name': 'saturated-fat', 'value': 10.0, '100g': 10.0, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'fruits-vegetables-nuts-estimate', 'value': 40.0, '100g': 40.0, 'serving': 40.0, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'sodium', 'value': 0.004000000189989805, '100g': 0.004000000189989805, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'salt', 'value': 0.009999999776482582, '100g': 0.009999999776482582, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'fat', 'value': 48.0, '100g': 48.0, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'cocoa', 'value': 17.520000457763672, '100g': 17.520000457763672, 'serving': 17.520000457763672, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'carbohydrates', 'value': 36.0, '100g': 36.0, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'proteins', 'value': 8.0, '100g': 8.0, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'sugars', 'value': 32.0, '100g': 32.0, 'serving': None, 'unit': 'g', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'energy', 'value': 617.0, '100g': 2582.0, 'serving': None, 'unit': 'kcal', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'nutrition-score-fr', 'value': None, '100g': 25.0, 'serving': None, 'unit': None, 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None},
    {'name': 'energy-kcal', 'value': 617.0, '100g': 617.0, 'serving': None, 'unit': 'kcal', 'prepared_value': None, 'prepared_100g': None, 'prepared_serving': None, 'prepared_unit': None}
]

def extract_nutrients(nutrient_list):
    wanted = {"energy-kcal", "proteins", "fat", "carbohydrates", "sugars", "fiber", "salt", "sodium"}
    result = {}

    for n in nutrient_list or []:
        name = n.get("name")
        if name in wanted:
            result[name] = n.get("100g")  # or "value"

    return result

# Test the extraction
extracted = extract_nutrients(nutriments_example)
print("Extracted nutrients:", extracted)

# Simulate adding to df
df = pd.DataFrame({'nutriments': [nutriments_example]})
df['nutrients_extracted'] = df['nutriments'].apply(extract_nutrients)
df['energy-kcal_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('energy-kcal'))
df['proteins_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('proteins'))
df['fat_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('fat'))
df['carbohydrates_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('carbohydrates'))
df['sugars_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('sugars'))
df['fiber_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('fiber'))
df['salt_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('salt'))
df['sodium_100g'] = df['nutrients_extracted'].apply(lambda x: x.get('sodium'))
df = df.drop(columns=['nutriments', 'nutrients_extracted'])

print("DataFrame after extraction:")
print(df)