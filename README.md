# 🛒 OLIST E-Commerce Sales & Delivery Performance Dashboard

An end-to-end e-commerce analytics project using **SQL, Excel, Tableau, and Power BI** to analyze sales trends, customer behavior, seller distribution, payment methods, and delivery performance.

---

## 🚀 Project Overview

This project is a comprehensive **E-Commerce Analytics initiative** built using the Brazilian **OLIST public dataset**.  
The goal is to analyze **orders, revenue, customers, sellers, payments, logistics performance, and product contribution**.

The project demonstrates a **complete analytics lifecycle**:
- Raw CSV ingestion
- SQL-based data modeling & KPI creation
- Excel & Tableau exploratory analysis
- Power BI executive dashboards
- Business storytelling & insights

---

## 💡 Key Findings & Business Insights

### 🔍 Key Findings

1. **Strong Business Growth:** Orders and revenue increased significantly from 2016 to 2018.
2. **Weekday Dominance:** Weekdays generate higher order volume and revenue than weekends.
3. **High Delivery Success:** ~97% of orders are successfully delivered.
4. **Delivery Time Impacts Reviews:** Longer delivery days result in lower review scores.
5. **Geographic Concentration:** Few states dominate customer demand and seller supply.
6. **Revenue Skew:** Limited product categories contribute majority of revenue.
7. **Payment Preference:** Credit cards dominate payment methods.

---

### 📌 Actionable Recommendations

1. Improve logistics in high-delay states.
2. Optimize operations for weekday demand peaks.
3. Expand seller base in high-performing states.
4. Optimize or discontinue low-performing product categories.
5. Improve delivery SLAs to protect customer satisfaction.

---

## 🔍 Key Performance Indicators (KPIs)

| KPI | Description | Business Impact |
|---|---|---|
| Total Orders | Number of orders placed | Demand measurement |
| Total Revenue | Total payment value | Business growth |
| Average Order Value | Revenue per order | Pricing insight |
| Delivery Success Rate | Delivered ÷ Total orders | Operational efficiency |
| Avg Delivery Days | Mean delivery duration | Logistics performance |
| Avg Review Score | Customer satisfaction | Experience quality |

---

## 🛠️ Methodology & Workflow

### 1️⃣ Data Ingestion
- Imported raw CSV files into MySQL
- Initial inspection using Excel

### 2️⃣ Data Cleaning & Preparation (SQL)
- Standardized date formats
- Handled NULL delivery dates
- Aggregated payment values at order level
- Created analytical views

### 3️⃣ SQL-Based Analysis
- Database schema creation
- Order-level fact view creation
- KPI calculations using SQL

### 4️⃣ Exploratory Data Analysis
- Excel for validation & checks
- Tableau for relationship-based analysis

### 5️⃣ Dashboard Development
- Multi-page Power BI dashboard
- KPI cards, slicers, drill-downs

### 6️⃣ Business Storytelling
- Converted KPIs into actionable insights
- Executive-ready dashboards

---

## 🧰 Tools & Technologies Used

| Category | Tools Used | Purpose |
|---|---|---|
| Database & Analysis | MySQL (Workbench) | Data cleaning & KPI creation |
| Data Preparation | Microsoft Excel | Validation & aggregation |
| BI & Visualization | Tableau | Exploratory dashboarding |
| BI & Visualization | Power BI | Executive dashboards |
| Version Control | GitHub | Project documentation |

---

## 📊 Dashboards Developed

### 🔷 Power BI Dashboard (3 Pages)

**1️⃣ Sales Overview**
- Total Orders, Revenue, AOV
- Time-based trends
- Weekday vs Weekend analysis

**2️⃣ Customer & Seller Insights**
- Top customer states
- Top seller states
- Payment type distribution
- Geographic demand analysis

**3️⃣ Delivery & Performance**
- Delivery days vs review score
- Avg delivery time by weekday
- Top & bottom products by revenue

---

### 🔶 Tableau Dashboard
- Revenue & order trends
- Customer & seller distribution
- Payment behavior
- Delivery performance analysis

---

### 🟢 Excel Dashboard
- KPI summary
- Validation & consistency checks
- Preliminary exploratory analysis

---

## 📂 Repository Structure

| Folder | Content | Purpose |
| :--------------------- | :-------------------------------------------- | :----------------------------------------------------------------------------------------------------- |
| `01_Raw_Data` | `olist_*.csv` | Raw OLIST e-commerce datasets including orders, payments, customers, sellers, products, and reviews. |
| `02_SQL_Scripts` | `olist_ecommerce_analysis.sql` | SQL scripts for database creation, data cleaning, joins, order-level views, and KPI calculations. |
| `03_Excel_Dashboard` | `Olist_Excel_Dashboard.xlsx` | Excel-based dashboard used for initial analysis, validation, and KPI verification. |
| `04_Tableau_Dashboard` | `Olist_Store_Analysis_Dashboard.twbx` | Interactive Tableau dashboard for exploratory analysis and visual storytelling. |
| `05_Dashboard_PowerBI` | `Olist_Ecommerce_Analysis.pbix` | Multi-page Power BI dashboard visualizing sales, customers, sellers, delivery, and performance KPIs. |
| `06_Deliverables` | `Olist_Project_Presentation.pptx` | Final project presentation containing business insights and dashboard explanation. |
| `07_Visuals` | `dashboard_screenshots.png` | Exported images of Excel, Tableau, and Power BI dashboards for GitHub preview. |
| `README.md` | Project documentation | Complete project overview, methodology, KPIs, dashboards, insights, and conclusions. |



---

## 🧠 Limitations

• Dataset is historical (2016–2018)  
• No real-time transaction updates  
• External economic factors not included  
• Review data limited to available ratings  

---

## 🙋‍♂️ Author

Mohammad Faiz  
📧 Email: faiz288fz@gmail.com  
🔗 LinkedIn: https://www.linkedin.com/in/mohammad-faiz-51674a282/

---

## 📎 License

This project is intended for educational and portfolio purposes only.  
Attribution is required if reused.

---

## 🛠️ Tools Required to Run This Project

<p align="left">
  <!-- MySQL -->
  <img src="https://www.vectorlogo.zone/logos/mysql/mysql-icon.svg" width="45"/>

  <!-- Microsoft Excel -->
  <img src="https://upload.wikimedia.org/wikipedia/commons/7/73/Microsoft_Excel_2013-2019_logo.svg" width="45"/>

  <!-- Tableau -->
  <img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Tableau_Logo.png" width="110"/>

  <!-- Power BI -->
  <img src="https://upload.wikimedia.org/wikipedia/commons/c/cf/New_Power_BI_Logo.svg" width="45"/>

  <!-- PowerPoint -->
  <img src="https://upload.wikimedia.org/wikipedia/commons/3/3b/Microsoft_PowerPoint_Logo.png" width="45"/>
</p>

**Tools Used:**
- Microsoft Excel  
- MySQL Workbench  
- Tableau Desktop  
- Power BI Desktop  
- Microsoft PowerPoint


---

⭐ Skills Demonstrated:  
SQL, Data Cleaning, Data Modeling, KPI Development, Excel Analytics, Tableau, Power BI, Business Storytelling, End-to-End Data Analytics
