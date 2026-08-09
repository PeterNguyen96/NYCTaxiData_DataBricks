# Building the dashboard

The gold tables are designed so each tile is a one-line `SELECT`. Two ways to turn
them into a dashboard, depending on which Databricks edition you're on.

## Option A — Databricks SQL / Lakeview dashboard (Free Edition or paid workspace)

1. Run the three notebooks in `/notebooks` in order (01 → 02 → 03).
2. In the Databricks sidebar, go to **SQL → Dashboards → Create dashboard**, name it
   `NYC Taxi Overview`.
3. For each query block in [`dashboard_queries.sql`](dashboard_queries.sql), add a
   dataset (**Data** tab → **+ Create from SQL**) and paste the query in.
4. Add a visualization for each dataset and arrange into this layout:

   | Row | Tiles |
   |---|---|
   | 1 | 4 KPI counters: Total Trips, Total Revenue, Avg Fare, Avg Tip % |
   | 2 | Line chart: trips & revenue by day |
   | 3 | Bar chart: trips by hour of day · Bar chart: trips by day of week |
   | 4 | Bar chart: top 15 pickup zones · Donut chart: revenue by borough |
   | 5 | Pie chart: payment type breakdown · Bar chart: trip distance buckets |

5. Publish the dashboard and set a schedule if you want it to auto-refresh
   (Free/Community Edition compute permitting).
6. Take a screenshot and drop it into `/dashboard/screenshot.png` — link it from the
   root `README.md` so it shows up on the GitHub repo page.

## Option B — Notebook visualizations (legacy Community Edition, no Lakeview)

Legacy Community Edition doesn't include Databricks SQL dashboards. Instead:

1. Create a new notebook `04_dashboard_notebook.py`.
2. For each query in `dashboard_queries.sql`, run it with `spark.sql(...)` and call
   `display(df)`, then use the built-in chart button on the result to pick a chart
   type (bar/line/pie) per the layout table above.
3. Databricks notebooks support **Dashboard view** (View menu → Dashboard), which
   lets you arrange those chart outputs into a single dashboard-style page within
   the notebook.
4. Export that view as a screenshot for the README.
