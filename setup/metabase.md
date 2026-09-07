# Metabase BI Dashboards & Visualization

Metabase serves as the open-source Business Intelligence (BI) layer, connecting directly to the ClickHouse `report` database to provide real-time dashboards for executives and marketing teams.

---

## 1. Initial Metabase Connection Setup

1. Open `http://localhost:3000` in your web browser.
2. Complete the initial admin account registration.
3. Add ClickHouse as a database connection:
   - **Database Type**: ClickHouse
   - **Display Name**: `E-Commerce Data Warehouse`
   - **Host**: `clickhouse` (Docker service name)
   - **Port**: `8123`
   - **Database Name**: `report`
   - **Username**: `default`
   - **Password**: *(leave blank)*
4. Click **Save** to begin schema synchronization.

---

## 2. Dashboard Blueprints

### Executive Overview Dashboard (Ban Lãnh Đạo)
- **Source View**: `report.view_overview_sales` & `report.view_overview_orders`
- **Key Metrics (KPI Cards)**:
  - Total Gross Merchandise Value (GMV): `SUM(total_gmv)`
  - Total Net Revenue: `SUM(total_net_revenue)`
  - Completed Orders: `SUM(order_count)`
- **Visualizations**:
  - Revenue Trend by Day/Month (Line chart)
  - Geographic Revenue Distribution by City/Province (Bar / Map chart)
  - Order Status Breakdown (Donut chart)
  - Payment & Shipping Method Share (Pie chart)

### Marketing Performance Dashboard (Bộ Phận Tiếp Thị)
- **Source View**: `report.view_marketing_dashboard`
- **Key Metrics**:
  - Average Order Value (AOV): `SUM(total_gmv) / SUM(order_count)`
  - Average Item Price: `SUM(total_gmv) / SUM(quantity_sold)`
  - Promotional Discount Volume: `SUM(total_gmv) - SUM(total_net_revenue)`
- **Visualizations**:
  - Campaign Performance Comparison (Grouped bar chart: GMV vs Net Revenue)
  - Top 10 Bestselling Products by Revenue (Horizontal bar chart)
  - Revenue Contribution by Category / Subcategory (Treemap / Pie chart)

---

## 3. Real-Time Refresh Configuration

In Metabase dashboard settings:
- Set auto-refresh interval to **1 minute** or **5 minutes** to observe continuous real-time changes as Airflow simulation DAGs populate new transactions.
