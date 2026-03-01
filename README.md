# 📊 Data Warehouse & Analytics Project

This repository contains an end-to-end SQL Data Warehouse project.  
It demonstrates how raw data is transformed into business-ready analytical models using a structured Medallion Architecture approach (Bronze, Silver, Gold).

The project focuses on practical implementation of data engineering, modeling, and analytics using SQL Server.

## 🏗️ Architecture

The warehouse follows a three-layer Medallion Architecture:

### 🥉 Bronze Layer
- Stores raw data from source systems (CSV files).
- Data is loaded into SQL Server without transformation.

### 🥈 Silver Layer
- Data cleansing and standardization.
- Integration between CRM and ERP datasets.
- Data quality handling.

### 🥇 Gold Layer
- Star schema design (Fact & Dimension tables).
- Business-ready data for reporting and analytics.
- Surrogate keys and referential integrity validation.

## 🎯 Project Objectives
- Build a structured SQL-based data warehouse.
- Integrate multiple source systems (ERP & CRM).
- Design analytical data models.
- Generate insights on:
  - Customer behavior  
  - Product performance  
  - Sales trends  

## 🛠️ Technologies Used
- SQL Server Express  
- SQL Server Management Studio (SSMS)  
- Draw.io (data modeling & architecture diagrams)  
- Git (version control)

## 📈 What This Project Demonstrates
- SQL Development  
- ETL & Data Transformation  
- Data Modeling (Star Schema)  
- Analytical Query Design  
- Data Warehouse Architecture  

## 📜 License
This project is licensed under the MIT License.
