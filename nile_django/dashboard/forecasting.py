import pandas as pd
import numpy as np
from django.db.models import Sum
from .models import Sale
import plotly.graph_objects as go
import plotly.io as pio


class ForecastingService:
    """
    Generates a 30-day sales forecast using weighted moving average
    and linear trend extrapolation. No external ML libraries needed.
    """

    @staticmethod
    def generate_forecast(days_ahead=30):
        """
        Returns a Plotly chart HTML string showing historical daily revenue
        and a 30-day forecast with a confidence band.
        """
        # 1. Pull historical daily revenue
        data = list(
            Sale.objects.values('order_date')
            .annotate(revenue=Sum('total_sales'))
            .order_by('order_date')
        )

        if not data or len(data) < 14:  # Need at least two weeks for seasonality
            return None

        df = pd.DataFrame(data)
        df.rename(columns={'order_date': 'date', 'revenue': 'y'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df['y'] = df['y'].astype(float)
        
        # Ensure a continuous date range to fill missing days with 0
        df.set_index('date', inplace=True)
        df = df.asfreq('D', fill_value=0)
        
        # 2. Fit Exponential Smoothing model (Holt-Winters)
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        
        # Using additive trend and weekly seasonality
        model = ExponentialSmoothing(
            df['y'],
            trend='add',
            seasonal='add',
            seasonal_periods=7,
            initialization_method="estimated"
        )
        fit_model = model.fit(optimized=True)
        
        # 3. Generate forecast
        forecast_y = fit_model.forecast(days_ahead)
        
        # 4. Generate forecast dates
        last_date = df.index[-1]
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=days_ahead)
        
        # 5. Confidence band (using root mean squared error)
        rmse = np.sqrt(fit_model.sse / len(df))
        # Simple widening confidence interval over time
        ci_multiplier = 1.96 * np.sqrt(np.arange(1, days_ahead + 1))
        upper_band = forecast_y + rmse * ci_multiplier
        lower_band = np.maximum(0, forecast_y - rmse * ci_multiplier) # prevent negative revenue

        # For plotting, reset index
        df.reset_index(inplace=True)

        # 6. Build Plotly figure
        fig = go.Figure()

        # Historical line
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['y'],
            mode='lines', name='Historical',
            line=dict(color='#111111', width=2),
            hovertemplate='%{x|%b %d}<br>$%{y:,.0f}<extra></extra>'
        ))

        # Forecast line
        fig.add_trace(go.Scatter(
            x=future_dates, y=forecast_y,
            mode='lines', name='Forecast',
            line=dict(color='#E63B2E', width=3, dash='dot'),
            hovertemplate='%{x|%b %d}<br>$%{y:,.0f}<extra>Forecast</extra>'
        ))

        # Confidence band
        fig.add_trace(go.Scatter(
            x=list(future_dates) + list(future_dates[::-1]),
            y=list(upper_band) + list(lower_band[::-1]),
            fill='toself',
            fillcolor='rgba(230, 59, 46, 0.08)',
            line=dict(color='rgba(0,0,0,0)'),
            name='Confidence Band',
            showlegend=True,
            hoverinfo='skip'
        ))

        fig.update_layout(
            title='Revenue_Forecast // 30_Day_Projection',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="JetBrains Mono, monospace", size=10, color='#52525B'),
            title_font=dict(family="Outfit, sans-serif", size=14, color='#111111'),
            margin=dict(t=50, b=40, l=50, r=30),
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom', y=1.02,
                xanchor='right', x=1,
                font=dict(size=9)
            ),
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=True, gridcolor='#F4F4F5', zeroline=False),
        )

        return pio.to_html(fig, full_html=False, include_plotlyjs=False)
