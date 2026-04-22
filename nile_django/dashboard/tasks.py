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
def scheduled_data_cleanup():
    """
    Example of a scheduled task.
    """
    # Logic to remove orphan records or old logs
    print("Performing scheduled data cleanup...")
    return "Success"
