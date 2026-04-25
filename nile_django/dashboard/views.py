from django.shortcuts import render, redirect
from django.db.models import Sum, Count, F
from django.db.models.functions import TruncDate
from django.contrib.auth.decorators import login_required
from .models import Customer, Product, Sale, DataUpload, AuditLog
import plotly.express as px
import plotly.io as pio
import json
import os
import time
import pandas as pd
from django.http import HttpResponse, JsonResponse
from django.utils import timezone
from django.contrib import messages
from .services import AnalyticsService
from .forecasting import ForecastingService
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_client_ip(request):
    x_forwarded = request.META.get('HTTP_X_FORWARDED_FOR')
    return x_forwarded.split(',')[0].strip() if x_forwarded else request.META.get('REMOTE_ADDR')


def log_action(user, action, detail='', request=None):
    AuditLog.objects.create(
        user=user if user and user.is_authenticated else None,
        action=action,
        detail=detail,
        ip_address=get_client_ip(request) if request else None,
    )


def get_dashboard_stats(queryset):
    total_rev = queryset.aggregate(Sum('total_sales'))['total_sales__sum'] or 0
    total_orders = queryset.values('order_id').distinct().count()
    total_customers = queryset.values('customer_id').distinct().count()
    return {
        'revenue': total_rev,
        'orders': total_orders,
        'customers': total_customers
    }


def generate_charts(queryset):
    data = list(queryset.select_related('customer', 'product').values(
        'order_date', 'total_sales', 'customer__region', 'product__name'
    ))
    df = pd.DataFrame(data)

    if df.empty:
        return {}

    df.rename(columns={
        'order_date': 'date',
        'total_sales': 'line_total',
        'customer__region': 'country',
        'product__name': 'description'
    }, inplace=True)

    layout_theme = {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'family': "JetBrains Mono, monospace", 'size': 10, 'color': '#52525B'},
        'margin': {'t': 40, 'b': 40, 'l': 40, 'r': 40},
        'hovermode': 'closest',
        'title_font': {'family': "Outfit, sans-serif", 'size': 14, 'color': '#111111'}
    }

    # 1. Sales Trends (Temporal Analysis)
    trend_df = df.groupby('date')['line_total'].sum().reset_index()
    fig_trend = px.line(trend_df, x='date', y='line_total', title='Sales_Trends //',
                        template='plotly_white', color_discrete_sequence=['#E63B2E'])
    fig_trend.update_layout(**layout_theme)
    fig_trend.update_traces(line=dict(width=3))

    # 2. Regional Spread (Extra feature)
    country_df = df.groupby('country')['line_total'].sum().sort_values(ascending=False).head(10).reset_index()
    fig_country = px.bar(country_df, x='country', y='line_total', title='Regional_Spread //',
                          template='plotly_white', color_discrete_sequence=['#111111'])
    fig_country.update_layout(**layout_theme)

    # 3. Product Revenue (Bar chart as requested)
    prod_df = df.groupby('description')['line_total'].sum().sort_values(ascending=False).head(10).reset_index()
    fig_prod = px.bar(prod_df, x='description', y='line_total', title='Product_Revenue //',
                       template='plotly_white', color_discrete_sequence=['#111111'])
    fig_prod.update_layout(**layout_theme)

    # 4. Customer Segmentation (Pie chart as requested)
    rfm_df = AnalyticsService.get_rfm_segments()
    if not rfm_df.empty:
        segment_counts = rfm_df['segment'].value_counts().reset_index()
        segment_counts.columns = ['segment', 'count']
        import plotly.graph_objects as go
        labels = segment_counts['segment'].tolist()
        values = [int(v) for v in segment_counts['count'].tolist()]
        
        fig_rfm = go.Figure(data=[go.Pie(
            labels=labels, 
            values=values, 
            hole=0.6,
            textinfo='percent',
            textposition='inside',
            marker=dict(colors=['#E63B2E', '#111111', '#52525B', '#A1A1AA', '#E4E4E7'])
        )])
        fig_rfm.update_layout(title='Customer_Segmentation //', **layout_theme)
    else:
        fig_rfm = None

    # 5. Forecasting
    chart_forecast = ForecastingService.generate_forecast()

    return {
        'chart_trend': pio.to_html(fig_trend, full_html=False, include_plotlyjs=False),
        'chart_country': pio.to_html(fig_country, full_html=False, include_plotlyjs=False),
        'chart_prod': pio.to_html(fig_prod, full_html=False, include_plotlyjs=False),
        'chart_rfm': pio.to_html(fig_rfm, full_html=False, include_plotlyjs=False) if fig_rfm else "",
        'chart_forecast': chart_forecast or "",
    }


# ─── Dashboard Views ─────────────────────────────────────────────────────────

@login_required
def dashboard_home(request):
    sales = Sale.objects.all()

    country = request.GET.get('country')
    category = request.GET.get('category')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    if country and country != 'All':
        sales = sales.filter(customer__region=country)
    if category and category != 'All':
        sales = sales.filter(product__category=category)
    if start_date:
        sales = sales.filter(order_date__gte=start_date)
    if end_date:
        sales = sales.filter(order_date__lte=end_date)

    stats = get_dashboard_stats(sales)
    charts = generate_charts(sales)
    countries = Customer.objects.values_list('region', flat=True).distinct().order_by('region')
    categories = Product.objects.values_list('category', flat=True).distinct().order_by('category')

    context = {
        'stats': stats,
        'charts': charts,
        'countries': countries,
        'categories': categories,
        'selected_country': country or 'All',
        'selected_category': category or 'All',
        'start_date': start_date or '',
        'end_date': end_date or '',
    }

    if request.htmx:
        return render(request, 'dashboard/partials/charts_content.html', context)

    return render(request, 'dashboard/index.html', context)


@login_required
def export_report(request, format):
    country = request.GET.get('country')
    sales = Sale.objects.all()
    if country and country != 'All':
        sales = sales.filter(customer__region=country)

    log_action(request.user, AuditLog.ACTION_EXPORT, f'Exported {format.upper()} report', request)

    if format == 'csv':
        content = AnalyticsService.generate_csv_report(sales)
        response = HttpResponse(content, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="nile_sales_report_{timezone.now().strftime("%Y%m%d")}.csv"'
        return response

    elif format == 'excel':
        content = AnalyticsService.generate_excel_report(sales)
        response = HttpResponse(content, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename="nile_sales_report_{timezone.now().strftime("%Y%m%d")}.xlsx"'
        return response

    return HttpResponse("Invalid Format", status=400)


# ─── Admin Control Center ────────────────────────────────────────────────────

@login_required
def control_center(request):
    """Admin-level data operations hub: file upload + ETL trigger."""
    if not request.user.is_admin_user:
        messages.error(request, 'Admin clearance required for the Control Center.')
        return redirect('dashboard_home')

    uploads = DataUpload.objects.filter(uploaded_by=request.user)[:20]
    context = {
        'uploads': uploads,
        'total_sales': Sale.objects.count(),
        'total_customers': Customer.objects.count(),
        'total_products': Product.objects.count(),
    }
    return render(request, 'dashboard/control_center.html', context)


@login_required
def upload_data(request):
    if request.method != 'POST' or not request.FILES.getlist('file'):
        return JsonResponse({'error': 'No file provided.'}, status=400)

    uploaded_files = request.FILES.getlist('file')
    success_count = 0
    fail_count = 0
    from dashboard.tasks import process_data_upload
    from dashboard.etl.pipeline import ETLPipeline

    for uploaded_file in uploaded_files:
        filename = uploaded_file.name

        if not filename.endswith(('.csv', '.xlsx')):
            messages.error(request, f"Skipped {filename}: Only CSV and Excel files are accepted.")
            fail_count += 1
            continue

        # Save upload record
        upload = DataUpload.objects.create(
            file=uploaded_file,
            original_filename=filename,
            uploaded_by=request.user,
            status=DataUpload.STATUS_PENDING,
        )

        log_action(request.user, AuditLog.ACTION_UPLOAD, f'Uploaded {filename}', request)

        pipeline = ETLPipeline(upload.file.path)
        try:
            preview = pipeline.get_mapping_preview()
            
            # If confidence is less than 80%, redirect to ETL wizard
            if preview['confidence'] < 0.8:
                return redirect('review_mapping', upload_id=upload.id)
                
            # Otherwise, auto-map and trigger Celery Task
            upload.column_mapping = preview['mapping']
            upload.save()
            process_data_upload.delay(upload.id)
            success_count += 1
        except Exception as e:
            upload.status = DataUpload.STATUS_FAILED
            upload.error_message = str(e)
            upload.save()
            messages.error(request, f'Upload failed for {filename}: {str(e)}')
            fail_count += 1

    if success_count > 0:
        messages.success(request, f'Successfully queued {success_count} file(s) for background processing.')

    return redirect('control_center')


@login_required
def review_mapping(request, upload_id):
    """The ETL Wizard: Allows user to correct column mappings."""
    from dashboard.etl.pipeline import ETLPipeline
    upload = get_object_or_404(DataUpload, id=upload_id)
    
    if request.method == 'POST':
        # Process the submitted mapping
        mapping = {}
        for key, value in request.POST.items():
            if key.startswith('map_') and value:
                expected_col = key[4:]  # remove 'map_' prefix
                mapping[value] = expected_col # actual -> expected
                
        upload.column_mapping = mapping
        upload.save()
        
        # Trigger Celery Task
        from dashboard.tasks import process_data_upload
        process_data_upload.delay(upload.id)
        
        messages.success(request, f'Mapping confirmed. Processing {upload.original_filename} in the background.')
        return redirect('control_center')
        
    # GET request: generate preview
    pipeline = ETLPipeline(upload.file.path)
    preview = pipeline.get_mapping_preview()
    
    preview_data = []
    for actual in preview['headers']:
        preview_data.append({
            'actual': actual,
            'mapped_to': preview['mapping'].get(actual, '')
        })
        
    context = {
        'upload': upload,
        'preview_data': preview_data,
        'expected': preview['expected']
    }
    return render(request, 'dashboard/confirm_mapping.html', context)




# ─── Security Telemetry (Audit Log) ──────────────────────────────────────────

@login_required
def audit_log_view(request):
    """Security telemetry dashboard showing all platform activity."""
    if not request.user.is_admin_user:
        messages.error(request, 'Admin clearance required.')
        return redirect('dashboard_home')

    logs = AuditLog.objects.select_related('user').all()[:100]
    context = {'logs': logs}
    return render(request, 'dashboard/audit_log.html', context)


# ─── JWT-Protected API Endpoints ─────────────────────────────────────────────

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def api_dashboard_stats(request):
    """GET /api/dashboard/stats/ — Returns key metrics for API consumers."""
    sales = Sale.objects.all()
    country = request.query_params.get('region')
    if country and country != 'All':
        sales = sales.filter(customer__region=country)
    stats = get_dashboard_stats(sales)
    return Response(stats)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def api_trigger_etl(request):
    """POST /api/dashboard/etl/trigger/ — Admin-only ETL trigger."""
    if not request.user.is_admin_user:
        return Response({'error': 'Admin privileges required.'}, status=403)

    from dashboard.etl.pipeline import ETLPipeline

    file_path = request.data.get('file_path', '')
    if not file_path or not os.path.exists(file_path):
        return Response({'error': f'File not found: {file_path}'}, status=400)

    try:
        pipeline = ETLPipeline(file_path)
        pipeline.run()
        log_action(request.user, AuditLog.ACTION_ETL_TRIGGER, f'API ETL: {file_path}', request)
        return Response({
            'status': 'success',
            'message': f'ETL pipeline completed. {Sale.objects.count()} total sale records.',
        })
    except Exception as e:
        return Response({'error': str(e)}, status=500)
