import polars as pl
from datetime import datetime
from pathlib import Path

class AnalyticsService:
    @staticmethod
    def process_sales_data(file_path: Path):
        # Load data (Polars is extremely fast)
        if file_path.suffix == '.csv':
            df = pl.read_csv(file_path)
        else:
            df = pl.read_excel(file_path)

        # Standardizing column names (Lower and snake_case)
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]

        # Basic cleansing
        df = df.drop_nulls()
        
        # Ensure date format
        if 'date' in df.columns:
            df = df.with_columns(pl.col('date').str.to_date(strict=False))
        elif 'order_date' in df.columns:
            df = df.with_columns(pl.col('order_date').str.to_date(strict=False))

        # Calculate Total Revenue if not exists
        if 'total_revenue' not in df.columns and 'price' in df.columns and 'quantity' in df.columns:
            df = df.with_columns((pl.col('price') * pl.col('quantity')).alias('total_revenue'))

        return df

    @staticmethod
    def get_sales_trends(df: pl.DataFrame):
        # Daily sales trend
        date_col = 'date' if 'date' in df.columns else 'order_date'
        trends = df.group_by(date_col).agg(
            pl.col('total_revenue').sum().alias('revenue'),
            pl.count().alias('orders')
        ).sort(date_col)
        
        return trends.to_dicts()

    @staticmethod
    def get_product_revenue(df: pl.DataFrame):
        # Top products by revenue
        products = df.group_by('product_name').agg(
            pl.col('total_revenue').sum().alias('revenue')
        ).sort('revenue', descending=True).head(10)
        
        return products.to_dicts()

    @staticmethod
    def get_customer_segmentation(df: pl.DataFrame):
        # Simple RFM Analysis backend
        # For now, let's just do revenue per customer
        customers = df.group_by('customer_id').agg(
            pl.col('total_revenue').sum().alias('total_spend'),
            pl.count().alias('frequency')
        ).sort('total_spend', descending=True).head(10)
        
        return customers.to_dicts()

    @staticmethod
    def get_categorical_distribution(df: pl.DataFrame):
        if 'category' in df.columns:
            return df.group_by('category').agg(
                pl.col('total_revenue').sum().alias('revenue')
            ).to_dicts()
        return []
