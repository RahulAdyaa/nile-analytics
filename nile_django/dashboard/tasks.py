from celery import shared_task
from .services import AnalyticsService
import os
from django.conf import settings
from datetime import datetime
import pandas as pd
from .models import Sale

@shared_task
def scheduled_export_report():
    """
    Automated scheduled task to generate periodic Pandas reports:
    Monthly, Region-wise, and Product-wise breakdowns.
    Exports to Excel and saves to media directory.
    """
    sales = Sale.objects.select_related('customer', 'product').all()
    if not sales.exists():
        return "No sales data to export."

    data = list(sales.values(
        'order_date', 'total_sales', 'profit', 
        'customer__region', 'product__category', 'product__name'
    ))
    df = pd.DataFrame(data)
    df['order_date'] = pd.to_datetime(df['order_date'])

    # 1. Monthly Summary
    monthly_df = df.set_index('order_date').resample('ME').agg({
        'total_sales': 'sum',
        'profit': 'sum'
    }).reset_index()

    # 2. Region-wise Breakdown
    region_df = df.groupby('customer__region').agg({
        'total_sales': 'sum',
        'profit': 'sum'
    }).reset_index()

    # 3. Product-wise Breakdown
    product_df = df.groupby(['product__category', 'product__name']).agg({
        'total_sales': 'sum',
        'profit': 'sum'
    }).reset_index()

    # Ensure output directory exists
    reports_dir = os.path.join(settings.BASE_DIR, 'media', 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    filename = f"nile_automated_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    filepath = os.path.join(reports_dir, filename)

    # Export to Excel with multiple sheets
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        monthly_df.to_excel(writer, sheet_name='Monthly Summary', index=False)
        region_df.to_excel(writer, sheet_name='Regional Breakdown', index=False)
        product_df.to_excel(writer, sheet_name='Product Breakdown', index=False)
        df.to_excel(writer, sheet_name='Raw Data', index=False)

    return f"Report successfully generated at {filepath}"

@shared_task
def process_data_upload(upload_id, wipe_existing=False):
    """
    Background task to process uploaded data via ETLPipeline.
    """
    from .models import DataUpload
    from .etl.pipeline import ETLPipeline
    import time
    import traceback

    try:
        upload = DataUpload.objects.get(id=upload_id)
        upload.status = DataUpload.STATUS_PROCESSING
        upload.save()

        start_time = time.time()
        
        pipeline = ETLPipeline(upload.file.path, upload.column_mapping)
        pipeline.run(wipe_existing=wipe_existing)

        upload.status = DataUpload.STATUS_SUCCESS
        upload.rows_processed = len(pipeline.final_df) if pipeline.final_df is not None else 0
        upload.processing_time_ms = int((time.time() - start_time) * 1000)
        upload.save()
        return "Success"
    except Exception as e:
        upload.status = DataUpload.STATUS_FAILED
        upload.error_message = f"{str(e)}\n{traceback.format_exc()}"
        upload.save()
        return f"Failed: {str(e)}"
