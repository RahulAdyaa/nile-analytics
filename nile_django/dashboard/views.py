from django.shortcuts import render, redirect, get_object_or_404
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
import shutil
from django.conf import settings
from django.core.files import File
from django.http import HttpResponse, JsonResponse
from django.utils import timezone
from django.contrib import messages
from .services import AnalyticsService
from .forecasting import ForecastingService
from django.urls import reverse
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
    from django.utils import timezone
    from datetime import timedelta
    
    # Current period metrics
    total_rev = float(queryset.aggregate(Sum('total_sales'))['total_sales__sum'] or 0)
    total_profit = float(queryset.aggregate(Sum('profit'))['profit__sum'] or 0)
    total_orders = queryset.count()
    total_customers = queryset.values('customer').distinct().count()
    
    avg_margin = (total_profit / total_rev * 100) if total_rev > 0 else 0
    
    # Real period-over-period growth from actual data
    # Compare last 30 days of data vs the preceding 30 days
    from django.db.models import Max
    latest_date = queryset.aggregate(Max('order_date'))['order_date__max']
    
    if latest_date:
        period_end = latest_date
        period_start = period_end - timedelta(days=30)
        prev_period_start = period_start - timedelta(days=30)
        
        current_rev = float(queryset.filter(
            order_date__gt=period_start, order_date__lte=period_end
        ).aggregate(Sum('total_sales'))['total_sales__sum'] or 0)
        
        prev_rev = float(queryset.filter(
            order_date__gt=prev_period_start, order_date__lte=period_start
        ).aggregate(Sum('total_sales'))['total_sales__sum'] or 0)
        
        current_vol = queryset.filter(
            order_date__gt=period_start, order_date__lte=period_end
        ).count()
        
        prev_vol = queryset.filter(
            order_date__gt=prev_period_start, order_date__lte=period_start
        ).count()
        
        revenue_growth = ((current_rev - prev_rev) / prev_rev * 100) if prev_rev > 0 else 0
        volume_growth = ((current_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
    else:
        revenue_growth = 0
        volume_growth = 0
    
    return {
        'revenue': total_rev,
        'profit': total_profit,
        'avg_margin': avg_margin,
        'orders': total_orders,
        'customers': total_customers,
        'revenue_growth': revenue_growth,
        'volume_growth': volume_growth
    }


def generate_charts(queryset):
    import plotly.graph_objects as go
    
    data = list(queryset.select_related('customer', 'product').values(
        'order_date', 'total_sales', 'profit', 'customer__region', 'product__name',
        'customer__age', 'product__category', 'returned', 'payment_mode', 'quantity'
    ))
    df = pd.DataFrame(data)

    if df.empty:
        return {}

    # CRITICAL: Force ALL numeric columns to native Python float
    # Django returns Decimal objects which Plotly cannot serialize properly
    df['total_sales'] = df['total_sales'].apply(lambda x: float(x) if x is not None else 0.0)
    df['profit'] = df['profit'].apply(lambda x: float(x) if x is not None else 0.0)
    df['quantity'] = df['quantity'].apply(lambda x: float(x) if x is not None else 0.0)

    df.rename(columns={
        'order_date': 'date',
        'total_sales': 'line_total',
        'customer__region': 'country',
        'product__name': 'description',
        'customer__age': 'age',
        'product__category': 'category'
    }, inplace=True)

    layout_theme = {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'family': "JetBrains Mono, monospace", 'size': 10, 'color': '#52525B'},
        'margin': {'t': 40, 'b': 40, 'l': 40, 'r': 40},
        'hovermode': 'closest',
        'title_font': {'family': "Outfit, sans-serif", 'size': 14, 'color': '#111111'}
    }

    # 1. Sales Trends — daily revenue grouped from DB
    trend_df = df.groupby('date')['line_total'].sum().reset_index().sort_values('date')
    trend_x = trend_df['date'].tolist()
    trend_y = [float(v) for v in trend_df['line_total'].tolist()]
    
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=trend_x, y=trend_y,
        mode='lines', fill='tozeroy',
        line=dict(width=3, shape='spline', color='#E63B2E'),
        fillcolor='rgba(230, 59, 46, 0.1)',
        hovertemplate="<b>Date:</b> %{x}<br><b>Revenue:</b> $%{y:,.2f}<extra></extra>"
    ))
    
    if trend_y:
        peak_idx = trend_y.index(max(trend_y))
        fig_trend.add_annotation(
            x=trend_x[peak_idx], y=trend_y[peak_idx],
            text=f"Peak: ${trend_y[peak_idx]:,.0f}",
            showarrow=True, arrowhead=1,
            bgcolor="#111111", font=dict(color="white", size=10),
            bordercolor="#E63B2E", borderpad=4,
            ax=0, ay=-40
        )
    
    fig_trend.update_layout(
        title='How are our sales performing over time?',
        **layout_theme,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='#F4F4F5'),
    )

    # 2. Regional Spread — revenue by region from DB
    country_df = df.groupby('country')['line_total'].sum().sort_values(ascending=False).head(10).reset_index()
    real_regions = country_df[country_df['country'] != 'Unknown']
    if not real_regions.empty:
        country_df = real_regions
    
    region_names = country_df['country'].tolist()
    region_values = [float(v) for v in country_df['line_total'].tolist()]
    region_colors = ['#E63B2E' if i == 0 else '#111111' for i in range(len(region_names))]
    
    fig_country = go.Figure(data=[go.Bar(
        x=region_names, y=region_values,
        marker_color=region_colors,
        hovertemplate="<b>Region:</b> %{x}<br><b>Total Revenue:</b> $%{y:,.2f}<extra></extra>"
    )])
    fig_country.update_layout(title='Which regions are driving the most revenue?', **layout_theme)

    # 3. Category Revenue — from DB groupby
    cat_df = df.groupby('category')['line_total'].sum().sort_values(ascending=False).reset_index()
    real_cats = cat_df[~cat_df['category'].isin(['Unknown', 'Uncategorized'])]
    if not real_cats.empty:
        cat_df = real_cats
    
    cat_names = cat_df['category'].tolist()
    cat_values = [float(v) for v in cat_df['line_total'].tolist()]
    cat_colors = ['#E63B2E' if i == 0 else '#27272A' if i == 1 else '#52525B' for i in range(len(cat_names))]
    
    fig_prod = go.Figure(data=[go.Bar(
        x=cat_names, y=cat_values,
        marker_color=cat_colors,
        hovertemplate="<b>Category:</b> %{x}<br><b>Revenue:</b> $%{y:,.2f}<extra></extra>"
    )])
    fig_prod.update_layout(title='Which categories generate the most revenue?', **layout_theme)
    
    # 3.1 Demographics — Age histogram OR Payment Mode pie
    has_age_data = 'age' in df.columns and df['age'].notna().any() and (df['age'] != 0).any()
    if has_age_data:
        age_data = df[df['age'].notna() & (df['age'] > 0)]
        fig_age = px.histogram(age_data, x='age', nbins=20, 
                              title='Who is our typical customer?',
                              template='plotly_white', color_discrete_sequence=['#E63B2E'])
        fig_age.update_layout(**layout_theme)
        fig_age.update_traces(
            hovertemplate="<b>Age Range:</b> %{x}<br><b>Count:</b> %{y}<extra></extra>",
            marker=dict(line=dict(width=1, color='white'))
        )
    else:
        # Payment Mode pie — built with go.Pie using explicit float lists
        pay_df = df.groupby('payment_mode')['line_total'].sum().sort_values(ascending=False).reset_index()
        real_pay = pay_df[~pay_df['payment_mode'].isin(['Unknown', '0'])]
        if not real_pay.empty:
            pay_df = real_pay
        
        pay_labels = pay_df['payment_mode'].tolist()
        pay_values = [float(v) for v in pay_df['line_total'].tolist()]
        
        pay_colors = {
            'Credit Card': '#E63B2E',
            'Debit Card': '#111111',
            'Cod': '#52525B',
            'Upi': '#A1A1AA',
            'Paypal': '#3B82F6',
            'Wallet': '#F59E0B',
        }
        
        fig_age = go.Figure(data=[go.Pie(
            labels=pay_labels,
            values=pay_values,
            hole=0.4,
            textinfo='label+percent',
            textposition='outside',
            marker=dict(
                colors=[pay_colors.get(l, '#D1D5DB') for l in pay_labels],
                line=dict(color='white', width=2)
            ),
            hovertemplate="<b>%{label}</b><br>Revenue: $%{value:,.2f}<br>Share: %{percent}<extra></extra>"
        )])
        fig_age.update_layout(title='Revenue by Payment Method', **layout_theme)

    # 3.2 Logistics — Returns by category OR Profit by category
    has_returns = 'returned' in df.columns and df['returned'].any()
    if has_returns:
        return_df = df[df['returned'] == True].groupby('category').size().reset_index(name='return_count')
        real_ret = return_df[~return_df['category'].isin(['Unknown', 'Uncategorized'])]
        if not real_ret.empty:
            return_df = real_ret
        
        ret_names = return_df['category'].tolist()
        ret_values = [int(v) for v in return_df['return_count'].tolist()]
        
        fig_return = go.Figure(data=[go.Bar(
            x=ret_names, y=ret_values,
            marker_color='#111111',
            hovertemplate="<b>Category:</b> %{x}<br><b>Returns:</b> %{y}<extra></extra>"
        )])
        fig_return.update_layout(title='Which categories have the most returns?', **layout_theme)
    else:
        # Profit by category — explicit float lists
        profit_df = df.groupby('category').agg(
            total_profit=('profit', 'sum'),
            total_revenue=('line_total', 'sum')
        ).reset_index()
        real_prof = profit_df[~profit_df['category'].isin(['Unknown', 'Uncategorized'])]
        if not real_prof.empty:
            profit_df = real_prof
        profit_df = profit_df.sort_values('total_profit', ascending=False)
        
        prof_names = profit_df['category'].tolist()
        prof_values = [float(v) for v in profit_df['total_profit'].tolist()]
        prof_colors = ['#16a34a' if v > 0 else '#E63B2E' for v in prof_values]
        
        fig_return = go.Figure(data=[go.Bar(
            x=prof_names, y=prof_values,
            marker_color=prof_colors,
            hovertemplate="<b>Category:</b> %{x}<br><b>Profit:</b> $%{y:,.2f}<extra></extra>"
        )])
        fig_return.update_layout(title='Which categories are most profitable?', **layout_theme)

    # 4. Customer Segmentation (Premium Donut)
    rfm_df = AnalyticsService.get_rfm_segments()
    if not rfm_df.empty:
        segment_counts = rfm_df['segment'].value_counts().reset_index()
        segment_counts.columns = ['segment', 'count']
        # Sort by count descending for clean visual flow
        segment_counts = segment_counts.sort_values('count', ascending=False)
        
        import plotly.graph_objects as go
        labels = segment_counts['segment'].tolist()
        values = [int(v) for v in segment_counts['count'].tolist()]
        total_customers = sum(values)
        
        # Curated color palette: green tones for healthy, warm for at-risk, dark for churned
        segment_colors = {
            'Champions': '#10B981',          # Emerald
            'Loyal Customers': '#34D399',    # Light emerald
            'Potential Loyalists': '#6EE7B7', # Soft mint
            'New Customers': '#3B82F6',       # Blue
            'At Risk': '#F59E0B',             # Amber warning
            "Can't Lose Them": '#EF4444',     # Red alert
            'Lost': '#1F2937',                # Dark slate
            'Others': '#9CA3AF',              # Cool gray
        }
        colors = [segment_colors.get(s, '#D1D5DB') for s in labels]
        
        # Slightly pull out the top segment for emphasis
        pull_values = [0.05 if i == 0 else 0 for i in range(len(labels))]
        
        fig_rfm = go.Figure(data=[go.Pie(
            labels=labels, 
            values=values, 
            hole=0.65,
            textinfo='percent',
            textposition='inside',
            textfont=dict(size=11, color='white', family='JetBrains Mono, monospace'),
            insidetextorientation='radial',
            pull=pull_values,
            marker=dict(
                colors=colors,
                line=dict(color='white', width=2)
            ),
            hovertemplate="<b>%{label}</b><br>Customers: %{value:,}<br>Share: %{percent}<extra></extra>",
            sort=False,
        )])
        
        fig_rfm.update_layout(
            title='How is our customer base segmented?',
            **layout_theme,
            showlegend=True,
            legend=dict(
                orientation='v',
                yanchor='middle', y=0.5,
                xanchor='left', x=1.02,
                font=dict(size=10, family='JetBrains Mono, monospace'),
                itemsizing='constant',
                tracegroupgap=2,
            ),
            # Center annotation showing total
            annotations=[dict(
                text=f'<b>{total_customers:,}</b><br><span style="font-size:9px;color:#9CA3AF">CUSTOMERS</span>',
                x=0.44, y=0.5,
                font=dict(size=22, family='Outfit, sans-serif', color='#111111'),
                showarrow=False,
            )]
        )
    else:
        fig_rfm = None

    # 5. Forecasting
    chart_forecast = ForecastingService.generate_forecast()

    return {
        'chart_trend': pio.to_html(fig_trend, full_html=False, include_plotlyjs=False),
        'chart_country': pio.to_html(fig_country, full_html=False, include_plotlyjs=False),
        'chart_prod': pio.to_html(fig_prod, full_html=False, include_plotlyjs=False),
        'chart_rfm': pio.to_html(fig_rfm, full_html=False, include_plotlyjs=False) if fig_rfm else "",
        'chart_age': pio.to_html(fig_age, full_html=False, include_plotlyjs=False) if fig_age else "",
        'chart_return': pio.to_html(fig_return, full_html=False, include_plotlyjs=False) if fig_return else "",
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
    latest_upload = DataUpload.objects.filter(status=DataUpload.STATUS_SUCCESS).order_by('-uploaded_at').first()
    recent_sales = sales.select_related('customer', 'product').order_by('-order_date', '-id')[:10]
    
    # Human touch: Advanced Intelligence Layer
    from .services import AnalyticsService
    from .forecasting import ForecastingService
    
    rfm_segments = AnalyticsService.get_rfm_segments()
    rfm_summary = rfm_segments['segment'].value_counts().to_dict() if not rfm_segments.empty else {}
    
    forecast_html = ForecastingService.generate_forecast()
    
    context = {
        'stats': stats,
        'charts': charts,
        'countries': countries,
        'categories': categories,
        'selected_country': country or 'All',
        'selected_category': category or 'All',
        'start_date': start_date or '',
        'end_date': end_date or '',
        'latest_upload': latest_upload,
        'recent_sales': recent_sales,
        'rfm_summary': rfm_summary,
        'forecast_html': forecast_html,
    }

    if request.htmx:
        return render(request, 'dashboard/partials/charts_content.html', context)

    return render(request, 'dashboard/index.html', context)


@login_required
def export_report(request, format):
    country = request.GET.get('country')
    category = request.GET.get('category')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    sales = Sale.objects.all()
    if country and country != 'All':
        sales = sales.filter(customer__region=country)
    if category and category != 'All':
        sales = sales.filter(product__category=category)
    if start_date:
        sales = sales.filter(order_date__gte=start_date)
    if end_date:
        sales = sales.filter(order_date__lte=end_date)

    from .services import AnalyticsService
    if format == 'csv':
        content = AnalyticsService.generate_csv_report(sales)
        filename = f"nile_report_{timezone.now():%Y%m%d}.csv"
        content_type = 'text/csv'
    else:
        content = AnalyticsService.generate_excel_report(sales)
        filename = f"nile_report_{timezone.now():%Y%m%d}.xlsx"
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    if not content:
        messages.error(request, "No data available to export.")
        return redirect('dashboard')

    response = HttpResponse(content, content_type=content_type)
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


@login_required
def flag_sale(request, sale_id):
    """Toggle the manual review flag on a sale via HTMX."""
    sale = get_object_or_404(Sale, id=sale_id)
    sale.is_flagged = not sale.is_flagged
    sale.save()
    
    # Return the updated button/status wrapped in the HTMX div
    color = "text-brand-red" if sale.is_flagged else "text-brand-muted"
    icon = "✓" if sale.is_flagged else "!"
    
    # We must return the outer HTML element with the HTMX attributes so it remains interactive
    return HttpResponse(f"""
        <div hx-get="{reverse('flag_sale', args=[sale.id])}" hx-swap="outerHTML" class="cursor-pointer">
            <span class="font-bold {color} hover:scale-110 transition-transform">
                {icon}
            </span>
        </div>
    """)


# ─── Admin Control Center ────────────────────────────────────────────────────

@login_required
def control_center(request):
    """Admin-level data operations hub: file upload + ETL trigger."""
    if not request.user.is_admin_user:
        messages.error(request, 'Admin clearance required for the Control Center.')
        return redirect('dashboard_home')

    # List files from the server's data folder
    server_data_dir = os.path.join(settings.BASE_DIR, 'data')
    server_files = []
    if os.path.exists(server_data_dir):
        for f in os.listdir(server_data_dir):
            if f.endswith(('.csv', '.xlsx')):
                server_files.append(f)

    user_uploads = DataUpload.objects.filter(uploaded_by=request.user)
    uploads = user_uploads.order_by('-uploaded_at')[:20]
    processing_active = user_uploads.filter(status__in=['pending', 'processing']).exists()
    
    context = {
        'uploads': uploads,
        'processing_active': processing_active,
        'server_files': sorted(server_files),
        'total_sales': Sale.objects.count(),
        'total_customers': Customer.objects.count(),
        'total_products': Product.objects.count(),
    }
    return render(request, 'dashboard/control_center.html', context)


@login_required
def upload_data(request):
    if not request.user.is_admin_user:
        messages.error(request, 'Admin clearance required to upload data.')
        return redirect('dashboard_home')

    if request.method != 'POST' or not request.FILES.getlist('file'):
        return JsonResponse({'error': 'No file provided.'}, status=400)

    uploaded_files = request.FILES.getlist('file')
    
    # Only process the first file to prevent redirect hijacking
    if len(uploaded_files) > 1:
        messages.warning(request, "Multiple files selected. Only the first file will be processed.")
        
    uploaded_file = uploaded_files[0]
    filename = uploaded_file.name

    if not filename.endswith(('.csv', '.xlsx')):
        messages.error(request, f"Skipped {filename}: Only CSV and Excel files are accepted.")
        return redirect('control_center')

    # Save upload record
    upload = DataUpload.objects.create(
        file=uploaded_file,
        original_filename=filename,
        uploaded_by=request.user,
        status=DataUpload.STATUS_PENDING,
    )

    log_action(request.user, AuditLog.ACTION_UPLOAD, f'Uploaded {filename}', request)

    from dashboard.etl.pipeline import ETLPipeline
    try:
        pipeline = ETLPipeline(upload.file.path)
        pipeline.get_mapping_preview()  # Validate file is readable
        
        # Redirect to mapping review page
        if request.headers.get('HX-Request'):
            response = HttpResponse()
            response['HX-Redirect'] = reverse('review_mapping', kwargs={'upload_id': upload.id})
            return response
        return redirect('review_mapping', upload_id=upload.id)
        
    except Exception as e:
        upload.status = DataUpload.STATUS_FAILED
        upload.error_message = str(e)
        upload.save()
        messages.error(request, f'Upload failed for {filename}: {str(e)}')

    if request.headers.get('HX-Request'):
        user_uploads = DataUpload.objects.filter(uploaded_by=request.user)
        uploads = user_uploads.order_by('-uploaded_at')[:20]
        processing_active = user_uploads.filter(status__in=['pending', 'processing']).exists()
        return render(request, 'dashboard/partials/upload_history.html', {
            'uploads': uploads,
            'processing_active': processing_active
        })

    return redirect('control_center')


@login_required
def process_server_file(request):
    """Handles ingestion of a file already present on the server."""
    if not request.user.is_admin_user:
        messages.error(request, 'Admin clearance required to ingest server files.')
        return redirect('dashboard_home')

    filename = request.POST.get('filename')
    if not filename:
        messages.error(request, "No file selected.")
        return redirect('control_center')

    server_file_path = os.path.join(settings.BASE_DIR, 'data', filename)
    if not os.path.exists(server_file_path):
        messages.error(request, f"File {filename} not found on server.")
        return redirect('control_center')

    # Register it as a DataUpload
    with open(server_file_path, 'rb') as f:
        upload = DataUpload.objects.create(
            original_filename=filename,
            uploaded_by=request.user,
            status=DataUpload.STATUS_PENDING,
        )
        upload.file.save(filename, File(f))
        upload.save()

    log_action(request.user, AuditLog.ACTION_UPLOAD, f'Selected server file: {filename}', request)

    from dashboard.etl.pipeline import ETLPipeline
    try:
        pipeline = ETLPipeline(upload.file.path)
        pipeline.get_mapping_preview()  # Validate file is readable
        
        # Redirect to mapping review page
        if request.headers.get('HX-Request'):
            response = HttpResponse()
            response['HX-Redirect'] = reverse('review_mapping', kwargs={'upload_id': upload.id})
            return response
        return redirect('review_mapping', upload_id=upload.id)
        
    except Exception as e:
        messages.error(request, f"ETL Pipeline Error: {str(e)}")

    if request.headers.get('HX-Request'):
        user_uploads = DataUpload.objects.filter(uploaded_by=request.user)
        uploads = user_uploads.order_by('-uploaded_at')[:20]
        processing_active = user_uploads.filter(status__in=['pending', 'processing']).exists()
        return render(request, 'dashboard/partials/upload_history.html', {
            'uploads': uploads,
            'processing_active': processing_active
        })

    return redirect('control_center')


@login_required
def review_mapping(request, upload_id):
    """The ETL Wizard: Allows user to correct column mappings."""
    if not request.user.is_admin_user:
        messages.error(request, 'Admin clearance required.')
        return redirect('dashboard_home')

    from dashboard.etl.pipeline import ETLPipeline
    upload = get_object_or_404(DataUpload, id=upload_id)
    
    if request.method == 'POST':
        # Process the submitted mapping
        mapping = {}
        for key, value in request.POST.items():
            if key.startswith('map_') and value:
                actual_file_col = key[4:]  # The header from the uploaded file
                expected_system_col = value # The standard system header selected
                mapping[actual_file_col] = expected_system_col
                
        upload.column_mapping = mapping
        upload.save()
        
        # Capture the Wipe Existing flag
        wipe_existing = request.POST.get('wipe_existing') == 'on'
        
        # Run in background thread to prevent blocking the UI
        import threading
        from dashboard.tasks import process_data_upload
        
        try:
            if getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False):
                threading.Thread(target=process_data_upload, args=(upload.id,), kwargs={'wipe_existing': wipe_existing}).start()
            else:
                process_data_upload.delay(upload.id, wipe_existing=wipe_existing)
        except Exception:
            threading.Thread(target=process_data_upload, args=(upload.id,), kwargs={'wipe_existing': wipe_existing}).start()
        
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
        messages.error(request, 'Admin clearance required to view audit logs.')
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
