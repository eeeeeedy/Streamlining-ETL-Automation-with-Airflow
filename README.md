# Streamlining ETL Automation with Airflow

Made by : [Edy Setiawan](https://www.linkedin.com/in/edysetiawan/)


# Table of Contents

1. [Introduction](#introduction)
2. [Project Objective](#project-objective)
3. [Data Source](#data-source)
4. [Technologies and Libraries Used](#technologies-and-libraries-used)
5. [ETL Automation Workflow](#etl-automation-workflow)
6. [Data Transformation](#data-transformation)
7. [Data Loading](#data-loading)

---

## Introduction

This project focuses on streamlining ETL (Extract, Transform, Load) automation using Apache Airflow. The ETL process is designed to automate the transformation and loading of data from a PostgreSQL database to Elasticsearch.

---

## Project Objective

The main objective is to automate the ETL process for a dataset that focuses on Company Bankruptcy Prediction, sourced from the Taiwan Economic Journal for the years 1999 to 2009.

---

## Data Source

The cleaned data used in this project is stored in `P2M3_Edy_Setiawan_data_clean.csv`, which contains various features related to company bankruptcy prediction.

---

## Technologies and Libraries Used

- Python
- Apache Airflow
- pandas
- PostgreSQL
- Elasticsearch

---

## ETL Automation Workflow

The ETL process is orchestrated using Apache Airflow, with the DAG defined in `P2M3_Edy_Setiawan_DAG.py`.

---

## Data Transformation

Data transformations are executed in Python, with the help of the pandas library, as outlined in `P2M3_Edy_Setiawan_GX.py`.

---

## Data Loading

The transformed data is loaded into Elasticsearch for further analysis and visualization.

---
