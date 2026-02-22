# Data Catalog — Gold Layer

## Overview
The Gold Layer represents the final business-ready data structure designed for analytics, reporting, and decision-making.  
It follows a **dimensional modeling approach**, consisting of **dimension tables** that store descriptive attributes and a **fact table** that records measurable business transactions.

---

## 1. gold.dim_customers

**Purpose:**  
Contains integrated customer data enriched with demographic and geographic attributes from multiple source systems.

### Columns

| Column Name      | Data Type     | Description |
|------------------|---------------|-------------|
| customer_key     | INT           | System-generated surrogate key used as the primary identifier within the dimension table. |
| customer_id      | INT           | Original numeric identifier assigned to each customer from the source system. |
| customer_number  | NVARCHAR(50)  | Business identifier for the customer, typically alphanumeric and used for tracking. |
| first_name       | NVARCHAR(50)  | Customer’s first or given name. |
| last_name        | NVARCHAR(50)  | Customer’s last or family name. |
| country          | NVARCHAR(50)  | Country of residence (e.g., Australia). |
| marital_status   | NVARCHAR(50)  | Marital status such as Married or Single. |
| gender           | NVARCHAR(50)  | Gender information (Male, Female, or n/a if unavailable). |
| birthdate        | DATE          | Customer birth date stored in YYYY-MM-DD format. |
| create_date      | DATE          | Date when the customer record was created in the system. |

---

## 2. gold.dim_products

**Purpose:**  
Provides descriptive product information including classification hierarchy and operational attributes used for analysis.

### Columns

| Column Name          | Data Type     | Description |
|----------------------|---------------|-------------|
| product_key          | INT           | Surrogate key uniquely identifying each product record. |
| product_id           | INT           | Internal identifier assigned to the product. |
| product_number       | NVARCHAR(50)  | Business product code used for referencing and inventory purposes. |
| product_name         | NVARCHAR(50)  | Descriptive name of the product including important characteristics. |
| category_id          | NVARCHAR(50)  | Identifier representing the product category. |
| category             | NVARCHAR(50)  | High-level classification (e.g., Bikes, Components). |
| subcategory          | NVARCHAR(50)  | More detailed grouping within the category. |
| maintenance_required | NVARCHAR(50)  | Indicates whether maintenance is required (Yes or No). |
| cost                 | INT           | Base cost of the product expressed in currency units. |
| product_line         | NVARCHAR(50)  | Product series or line (e.g., Road, Mountain). |
| start_date           | DATE          | Date when the product became active or available. |

---

## 3. gold.fact_sales

**Purpose:**  
Stores transactional sales data and acts as the central table for quantitative business analysis.

### Columns

| Column Name     | Data Type     | Description |
|-----------------|---------------|-------------|
| order_number    | NVARCHAR(50)  | Unique identifier for each sales order (e.g., SO54496). |
| product_key     | INT           | Foreign key referencing the product dimension table. |
| customer_key    | INT           | Foreign key referencing the customer dimension table. |
| order_date      | DATE          | Date when the order was placed. |
| shipping_date   | DATE          | Date when the order was shipped. |
| due_date        | DATE          | Payment due date for the order. |
| sales_amount    | INT           | Total monetary value of the transaction line item. |
| quantity        | INT           | Number of product units included in the transaction. |
| price           | INT           | Price per unit of the product in the transaction. |

---

## Notes
- Dimension tables are designed to provide descriptive context for analysis.
- The fact table contains measurable metrics linked to dimensions through surrogate keys.
- The Gold Layer is optimized for BI tools, dashboards, and reporting workloads.
