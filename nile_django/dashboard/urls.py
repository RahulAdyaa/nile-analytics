from django.urls import path
from . import views

urlpatterns = [
    # ─── Browser Routes (login_required) ─────────────────────────────────────
    path('', views.dashboard_home, name='dashboard_home'),
    path('export/<str:format>/', views.export_report, name='export_report'),
    path('control/', views.control_center, name='control_center'),
    path('control/upload/', views.upload_data, name='upload_data'),
    path('control/process-server-file/', views.process_server_file, name='process_server_file'),
    path('control/upload/<int:upload_id>/review/', views.review_mapping, name='review_mapping'),
    path('audit/', views.audit_log_view, name='audit_log'),

    # ─── JWT-Protected API Routes ─────────────────────────────────────────────
    path('api/dashboard/stats/', views.api_dashboard_stats, name='api_dashboard_stats'),
    path('api/dashboard/etl/trigger/', views.api_trigger_etl, name='api_trigger_etl'),
]
