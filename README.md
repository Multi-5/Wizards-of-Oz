# OT7 - Foundation of Data Engineering - 2025/2026

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: **Robert Michel, Stefan Severin**

### Abstract

## Datasets Description 

## Ingestion:
For this projects we utilize 2 datasources:
- OpenFoodFacs Dataset
- U.S. Department of Agriculture [(USDA) Dataset](https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_json_2025-04-24.zip)

Since the OpenFoodfacts dataset was a huge .parquet file (> 4 GB) it's not directly included in the repository.</br>
Please download the file 'food.parquet' from the the following website: [OpenFoodFacts](https://huggingface.co/datasets/openfoodfacts/product-database/viewer/default/food) </br>

The datafiles are stored first in the folder `airflow/data/dataunclean` before loading it into the pipeline folder `/landing` 

## Staging 

## Queries 
1. How does the total nutrient content (protein, magnesium, vitamins) of a homemade dish compare to that of a premade product?
  <details>
  <summary>SQL Query</summary>
  ```SELECT
    c.category_name,
    s.source_name,
    COUNT(*)                           AS food_count,
    AVG(f.energy_kcal)                 AS avg_energy_kcal,
    AVG(f.protein_g)                   AS avg_protein_g,
    AVG(f.carbohydrate_g)              AS avg_carbohydrate_g,
    AVG(f.fat_g)                       AS avg_fat_g,
    AVG(f.sugar_g)                     AS avg_sugar_g,
    AVG(f.magnesium_mg)                AS avg_magnesium_mg,
    AVG(f.vitamin_a_mcg + f.vitamin_c_mg + f.vitamin_d_mcg + f.vitamin_e_mg)
                                       AS avg_total_vitamins
FROM fact_food_nutrition f
JOIN dim_category c ON f.category_key = c.category_key
JOIN dim_source   s ON f.source_key   = s.source_key
WHERE s.source_name IN ('USDA','OpenFoodFacts')
GROUP BY c.category_name, s.source_name
ORDER BY c.category_name, s.source_name;```
  
  </details>
2. Which food categories show the largest fat content differences between raw and processed versions?
  <details>
  <summary>SQL Query</summary>
  ```SELECT c.category_name,
       AVG(CASE WHEN s.source_name = 'USDA' THEN f.fat_g END) AS avg_fat_usda,
       AVG(CASE WHEN s.source_name = 'OpenFoodFacts' THEN f.fat_g END) AS avg_fat_off,
       (AVG(CASE WHEN s.source_name = 'OpenFoodFacts' THEN f.fat_g END) - 
        AVG(CASE WHEN s.source_name = 'USDA' THEN f.fat_g END)) AS fat_difference
FROM fact_food_nutrition f
JOIN dim_category c ON f.category_key = c.category_key
JOIN dim_source s ON f.source_key = s.source_key
WHERE f.fat_g IS NOT NULL
GROUP BY c.category_name
HAVING COUNT(CASE WHEN s.source_name = 'USDA' THEN 1 END) > 0 
       AND COUNT(CASE WHEN s.source_name = 'OpenFoodFacts' THEN 1 END) > 0
       AND ABS(AVG(CASE WHEN s.source_name = 'OpenFoodFacts' THEN f.fat_g END) - 
               AVG(CASE WHEN s.source_name = 'USDA' THEN f.fat_g END)) > 0
ORDER BY ABS(AVG(CASE WHEN s.source_name = 'OpenFoodFacts' THEN f.fat_g END) - 
             AVG(CASE WHEN s.source_name = 'USDA' THEN f.fat_g END)) DESC
LIMIT 10;```
  
  </details>

3.How does sodium (salt) content differ between homemade and commercial dishes of the same type?
<details>
  <summary>SQL Query</summary>
  ```SELECT
    c.category_name AS dish_type,
    s.source_name   AS dataset,
    COUNT(*)        AS items,
    AVG(f.sodium_mg) AS avg_sodium_mg,
    MIN(f.sodium_mg) AS min_sodium_mg,
    MAX(f.sodium_mg) AS max_sodium_mg
FROM fact_food_nutrition f
JOIN dim_category c ON f.category_key = c.category_key
JOIN dim_source   s ON f.source_key   = s.source_key
WHERE s.source_name IN ('USDA','OpenFoodFacts')
GROUP BY c.category_name, s.source_name
HAVING AVG(f.sodium_mg) > 0
ORDER BY c.category_name, s.source_name;```
  
  </details>
4.How does sodium (salt) content differ between homemade and commercial dishes of the same type?


## Requirements

## How to run?
1. Ensure you have Docker installed and running.
2. Clone the repository.
3. Change into the folder `/airflow`.
4. Use `docker compose up`.
5. Visit: 

  | Service  | URL |
  | ------------- | ------------- |
  | Adminer  |  http://localhost:8081/ |
  | Airflow  | http://localhost:8082/  |
  | Neo4j  | http://localhost:7474/browser/  |


## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

