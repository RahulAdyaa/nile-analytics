# Nile Corporation: Python-Centric Sales Intelligence

A premium analytics dashboard built with **Django**, **HTMX**, and **Plotly Python**, using the real-world Business Intelligence dataset: `Online Retail II`.

## 🚀 Key Features
- **Full-Stack Python**: Logic and UI state managed primarily in Python via Django and Plotly.
- **Real-Time Interactivity**: HTMX-driven filters for instant dashboard updates without page reloads.
- **Modern Aesthetic**: High-fidelity glassmorphic design using Tailwind CSS.
- **Data Mastery**: Optimized parsing of Large Excel datasets using Pandas.

## 🛠 Setup & Run
### 1. Environment Setup
Clone the repository and enter the project directory:
```bash
cd nile_django
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy the template and update your credentials:
```bash
cp .env.example .env
```

### 3. Database & Data Load
Initialize the database and load the initial dataset:
```bash
python manage.py migrate
python manage.py load_retail_data
```

### 4. Start Server
```bash
python manage.py runserver
```
Visit `http://localhost:8000` to view the dashboard.

## 📊 Dataset Insight
The `online+retail+ii.zip` dataset contains all transactions occurring between 01/12/2009 and 09/12/2011 for a UK-based and registered non-store online retail. The platform provides insights into:
- **Daily Sales Trends**: Time-series analysis of revenue.
- **Geopolitical Performance**: Top revenue-generating countries.
- **Inventory Concentration**: Volume and revenue distribution by product inventory.
