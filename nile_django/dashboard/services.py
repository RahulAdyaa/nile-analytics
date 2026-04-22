import pandas as pd
from .models import Customer, Product, Sale
from django.db.models import Sum, F, Max
from django.utils import timezone
import io

class AnalyticsService:
    @staticmethod
    def get_rfm_segments():
        """
        Perform RFM Analysis on the normalized dataset.
        """
        # 1. Load Data from Sales
        queryset = Sale.objects.all().values(
            'customer_id', 'order_id', 'order_date', 'total_sales'
        )
        df = pd.DataFrame(queryset)
        
        if df.empty:
            return pd.DataFrame()

        # 2. Recency, Frequency, Monetary calculations
        # Reference date set to one day after the last order
        now = pd.to_datetime(df['order_date'].max()) + pd.Timedelta(days=1)
        df['order_date'] = pd.to_datetime(df['order_date'])
        
        rfm = df.groupby('customer_id').agg({
            'order_date': lambda x: (now - x.max()).days,
            'order_id': 'nunique',
            'total_sales': 'sum'
        }).rename(columns={
            'order_date': 'recency',
            'order_id': 'frequency',
            'total_sales': 'monetary'
        })
        
        # 3. Scoring (1-5, where 5 is best)
        try:
            rfm['r_score'] = pd.qcut(rfm['recency'].rank(method='first'), 5, labels=[5, 4, 3, 2, 1])
            rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
            rfm['m_score'] = pd.qcut(rfm['monetary'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
        except ValueError:
            rfm['r_score'] = 3
            rfm['f_score'] = 3
            rfm['m_score'] = 3

        rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)
        
        # 4. Segment Assignment
        def segment_it(row):
            score = row['rfm_score']
            if score in ['555', '554', '545', '455', '454', '544', '445']: return 'Champions'
            if score[0] >= '4' and score[1] >= '4': return 'Loyal Customers'
            if score[0] >= '4' and score[2] >= '4': return 'Potential Loyalists'
            if score[0] >= '4': return 'New Customers'
            if score[0] == '3' and score[2] >= '3': return 'At Risk'
            if score[0] <= '2' and score[2] >= '4': return "Can't Lose Them"
            if score[0] <= '2' and score[1] <= '2': return 'Lost'
            return 'Others'

        rfm['segment'] = rfm.apply(segment_it, axis=1)
        return rfm.reset_index()

    @staticmethod
    def generate_csv_report(queryset):
        # Flatten for export
        data = list(queryset.values(
            'order_id', 'order_date', 'customer__name', 'customer__region', 
            'customer__city', 'product__name', 'product__category', 
            'quantity', 'unit_price', 'total_sales', 'profit', 'payment_mode'
        ))
        df = pd.DataFrame(data)
        if df.empty:
            return None
        
        output = io.StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()

    @staticmethod
    def generate_excel_report(queryset):
        data = list(queryset.values(
            'order_id', 'order_date', 'customer__name', 'customer__region', 
            'customer__city', 'product__name', 'product__category', 
            'quantity', 'unit_price', 'total_sales', 'profit', 'payment_mode'
        ))
        df = pd.DataFrame(data)
        if df.empty:
            return None
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sales Report')
        return output.getvalue()
