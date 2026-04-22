# Nile Corporation: Python-Centric Sales Intelligence

A premium analytics dashboard built with **Django**, **HTMX**, and **Plotly Python**, using the real-world Business Intelligence dataset: `Online Retail II`.

## 🚀 Key Features
- **Full-Stack Python**: Logic and UI state managed primarily in Python via Django and Plotly.
- **Real-Time Interactivity**: HTMX-driven filters for instant dashboard updates without page reloads.
- **Modern Aesthetic**: High-fidelity glassmorphic design using Tailwind CSS.
- **Data Mastery**: Optimized parsing of Large Excel datasets using Pandas.

## 🛠 Setup & Run
### 1. Environment Setup
```bash
cd nile_django
source venv/bin/activate
pip install -r requirements.txt  # (Created requirements.txt below)
```

### 2. Database & Data Load
The data has already been loaded for you (10,000 records). If you wish to reload or add more:
```bash
python manage.py load_retail_data
```

### 3. Start Server
```bash
python manage.py runserver
```
Visit `http://localhost:8000` to view the dashboard.

## 📊 Dataset Insight
The `online+retail+ii.zip` dataset contains all transactions occurring between 01/12/2009 and 09/12/2011 for a UK-based and registered non-store online retail. The platform provides insights into:
- **Daily Sales Trends**: Time-series analysis of revenue.
- **Geopolitical Performance**: Top revenue-generating countries.
- **Inventory Concentration**: Volume and revenue distribution by product inventory.
