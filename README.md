# Nile Analytics // Active Stream Retail V2

A high-performance, real-time sales analytics dashboard built for enterprise data ingestion and telemetry.

## 🚀 Tech Stack
- **Backend:** Django 5 + Python 3
- **Database:** PostgreSQL 15 (Production) / SQLite (Local Dev)
- **Frontend UI:** TailwindCSS, HTML5, HTMX (for fast, asynchronous reloads without React overhead)
- **Data Engine:** Pandas, Plotly Express, Plotly Graph Objects (for real-time charting)
- **Architecture:** Monolithic, Server-Side Rendered (SSR)

## ⚡ Key Features
- **Zero-Friction Ingestion Pipeline**: Support for batch uploading multiple CSV/Excel datasets at once. The automated ETL pipeline auto-maps arbitrary column names into the system schema, providing bulletproof schema validation.
- **Real-Time Forecasting**: Built-in analytics engine utilizing numpy-based moving average and linear trend extrapolation for 30-day projections.
- **RFM Customer Segmentation**: Live Recency, Frequency, and Monetary (RFM) clustering to segment customers into actionable categories (e.g., *Champions*, *At Risk*, *Lost*).
- **Responsive Telemetry Dashboard**: A high-fidelity, brutalist-inspired dark/light interface designed for rapid situational awareness, featuring Regional Spread, Product Revenue, and Customer Segmentation visualizations.

## 🛠 Getting Started

### 1. Environment Setup
```bash
# Clone the repository
git clone <your-repository-url>
cd nile-corpo/nile_django

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Configuration
Ensure PostgreSQL is running. By default, the application looks for a Postgres database or falls back to SQLite.
```bash
# Run migrations
python manage.py makemigrations
python manage.py migrate

# Create an admin superuser
python manage.py createsuperuser
```

### 3. Running the Server
```bash
python manage.py runserver
```
Navigate to `http://localhost:8000`.

## 📊 Data Upload
To populate the dashboard:
1. Ensure you are logged in (the dashboard is restricted to authenticated users).
2. Click the **Upload Data** button on the top-right navigation bar.
3. Drag and drop your `.csv` or `.xlsx` files into the ingestion zone.
4. The backend will automatically map your columns, normalize the data, flush the old records if necessary, and render the new intelligence.
