from django.db import models
from django.conf import settings


class Customer(models.Model):
    name = models.CharField(max_length=255)
    region = models.CharField(max_length=100)
    city = models.CharField(max_length=100)

    class Meta:
        unique_together = ('name', 'region', 'city')

    def __str__(self):
        return self.name


class Product(models.Model):
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=100)
    sub_category = models.CharField(max_length=100)

    class Meta:
        unique_together = ('name', 'category', 'sub_category')

    def __str__(self):
        return self.name


class Sale(models.Model):
    order_id = models.CharField(max_length=50)
    order_date = models.DateField(db_index=True)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name='sales')
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name='sales')
    quantity = models.IntegerField()
    unit_price = models.DecimalField(max_digits=12, decimal_places=2)
    discount = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    total_sales = models.DecimalField(max_digits=15, decimal_places=2)
    profit = models.DecimalField(max_digits=15, decimal_places=2)
    payment_mode = models.CharField(max_length=100)

    class Meta:
        indexes = [
            models.Index(fields=['order_date', 'customer']),
            models.Index(fields=['customer', 'total_sales']),
            models.Index(fields=['product', 'total_sales']),
        ]

    def __str__(self):
        return f"Order {self.order_id} - {self.product.name}"


# ─── Data Upload Tracking ────────────────────────────────────────────────────

class DataUpload(models.Model):
    STATUS_PENDING = 'pending'
    STATUS_PROCESSING = 'processing'
    STATUS_SUCCESS = 'success'
    STATUS_FAILED = 'failed'

    STATUS_CHOICES = [
        (STATUS_PENDING, 'Pending'),
        (STATUS_PROCESSING, 'Processing'),
        (STATUS_SUCCESS, 'Success'),
        (STATUS_FAILED, 'Failed'),
    ]

    file = models.FileField(upload_to='uploads/%Y/%m/')
    original_filename = models.CharField(max_length=255)
    uploaded_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='uploads')
    uploaded_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    rows_processed = models.IntegerField(default=0)
    column_mapping = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True, default='')
    processing_time_ms = models.IntegerField(default=0)

    class Meta:
        ordering = ['-uploaded_at']

    def __str__(self):
        return f"{self.original_filename} ({self.get_status_display()})"


# ─── Audit Log ────────────────────────────────────────────────────────────────

class AuditLog(models.Model):
    ACTION_LOGIN = 'login'
    ACTION_LOGOUT = 'logout'
    ACTION_UPLOAD = 'upload'
    ACTION_ETL_TRIGGER = 'etl_trigger'
    ACTION_EXPORT = 'export'
    ACTION_REGISTER = 'register'

    ACTION_CHOICES = [
        (ACTION_LOGIN, 'User Login'),
        (ACTION_LOGOUT, 'User Logout'),
        (ACTION_UPLOAD, 'Data Upload'),
        (ACTION_ETL_TRIGGER, 'ETL Pipeline Triggered'),
        (ACTION_EXPORT, 'Report Exported'),
        (ACTION_REGISTER, 'User Registration'),
    ]

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name='audit_logs')
    action = models.CharField(max_length=30, choices=ACTION_CHOICES)
    detail = models.TextField(blank=True, default='')
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-timestamp']

    def __str__(self):
        return f"[{self.timestamp:%Y-%m-%d %H:%M}] {self.get_action_display()} by {self.user}"
