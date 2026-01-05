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

## Requirements

## How to run?
1. Ensure you have Docker installed and running.
2. Clone the repository.
3. Change into the folder `/airflow`.
4. Use `docker compose up`.


## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

