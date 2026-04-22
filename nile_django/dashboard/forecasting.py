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

        if not data or len(data) < 7:
            return None

        df = pd.DataFrame(data)
        df.rename(columns={'order_date': 'date', 'revenue': 'y'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df['y'] = df['y'].astype(float)
        df = df.sort_values('date').reset_index(drop=True)

        # 2. Linear Trend via Least Squares
        df['x'] = np.arange(len(df))
        slope, intercept = np.polyfit(df['x'], df['y'], 1)

        # 3. Weighted Moving Average (window=7, recent days weighted heavier)
        window = min(7, len(df))
        weights = np.arange(1, window + 1, dtype=float)
        weights /= weights.sum()
        wma = df['y'].rolling(window=window).apply(lambda vals: np.dot(vals, weights), raw=True)
        df['wma'] = wma

        # 4. Generate forecast dates
        last_date = df['date'].iloc[-1]
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=days_ahead)
        future_x = np.arange(len(df), len(df) + days_ahead)

        # Forecast = linear trend + residual correction from last WMA
        last_wma = df['wma'].dropna().iloc[-1] if df['wma'].dropna().any() else df['y'].iloc[-1]
        last_trend = slope * df['x'].iloc[-1] + intercept
        correction = last_wma - last_trend

        forecast_y = slope * future_x + intercept + correction

        # 5. Confidence band (±1 std of recent residuals)
        residuals = df['y'].iloc[-window:] - (slope * df['x'].iloc[-window:] + intercept)
        std_dev = residuals.std() if len(residuals) > 1 else df['y'].std() * 0.1
        upper_band = forecast_y + 1.5 * std_dev
        lower_band = forecast_y - 1.5 * std_dev

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
