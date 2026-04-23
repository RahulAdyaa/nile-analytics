from celery import shared_task
from .services import AnalyticsService
from .models import Transaction
import time

@shared_task
def generate_bulk_report(format='csv'):
    """
    Simulates a heavy report generation task.
    In a real app, this would save to S3 or a media folder and notify the user.
    """
    time.sleep(5)  # Simulate heavy work
    queryset = Transaction.objects.all()
    if format == 'csv':
        return AnalyticsService.generate_csv_report(queryset)
    return AnalyticsService.generate_excel_report(queryset)

@shared_task
def process_data_upload(upload_id):
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
        pipeline.run()

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
