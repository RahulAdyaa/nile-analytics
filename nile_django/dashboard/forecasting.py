import pandas as pd
import numpy as np
from django.db.models import Sum
from .models import Sale
import plotly.graph_objects as go
import plotly.io as pio
import warnings


class ForecastingService:
    """
    Generates a 30-day sales forecast from actual historical daily revenue.
    Uses Exponential Smoothing with automatic fallback to weighted moving average.
    """

    @staticmethod
    def generate_forecast(days_ahead=30):
        """
        Returns a Plotly chart HTML string showing historical daily revenue
        and a 30-day forecast with a confidence band.
        All values are derived from Sale.objects — zero hardcoded data.
        """
        # 1. Pull historical daily revenue from DB
        data = list(
            Sale.objects.values('order_date')
            .annotate(revenue=Sum('total_sales'))
            .order_by('order_date')
        )

        if not data or len(data) < 14:
            return None

        df = pd.DataFrame(data)
        df.rename(columns={'order_date': 'date', 'revenue': 'y'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        
        # CRITICAL: Force Decimal → native Python float
        df['y'] = df['y'].apply(lambda x: float(x) if x is not None else 0.0)
        
        # Ensure continuous date range (fill gaps with 0)
        df.set_index('date', inplace=True)
        df = df.asfreq('D', fill_value=0)
        
        # 2. Try Holt-Winters, fallback to moving average if it diverges
        forecast_y = None
        model_name = 'Weighted Moving Average'
        
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
            
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model = ExponentialSmoothing(
                    df['y'],
                    trend='add',
                    seasonal='add',
                    seasonal_periods=7,
                    initialization_method="estimated"
                )
                fit_model = model.fit(optimized=True, use_brute=True)
                forecast_y = fit_model.forecast(days_ahead)
                
                # Sanity check: forecast shouldn't be more than 3x the historical max
                hist_max = df['y'].max()
                hist_mean = df['y'].mean()
                if forecast_y.max() > hist_max * 3 or forecast_y.min() < -hist_mean:
                    # Model diverged — reject it
                    forecast_y = None
                else:
                    model_name = 'Holt-Winters'
                    rmse = np.sqrt(fit_model.sse / len(df))
        except Exception:
            forecast_y = None
        
        # 3. Fallback: Weighted moving average with trend
        if forecast_y is None:
            # Use last 30 days as the base period
            recent = df['y'].tail(30).values
            weights = np.linspace(0.5, 1.5, len(recent))
            weights /= weights.sum()
            weighted_avg = float(np.dot(recent, weights))
            
            # Calculate simple trend from last 60 days vs last 30 days
            if len(df) >= 60:
                prev_30 = df['y'].iloc[-60:-30].mean()
                last_30 = df['y'].iloc[-30:].mean()
                daily_trend = (last_30 - prev_30) / 30
            else:
                daily_trend = 0
            
            forecast_values = []
            for i in range(days_ahead):
                val = weighted_avg + daily_trend * (i + 1)
                forecast_values.append(max(0, val))
            forecast_y = pd.Series(forecast_values)
            
            # RMSE from recent data standard deviation
            rmse = float(df['y'].tail(30).std())
        
        # 4. Generate forecast dates
        last_date = df.index[-1]
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=days_ahead)
        
        # 5. Confidence band
        ci_multiplier = 1.96 * np.sqrt(np.arange(1, days_ahead + 1))
        upper_band = [float(v) for v in (forecast_y.values + rmse * ci_multiplier)]
        lower_band = [max(0, float(v)) for v in (forecast_y.values - rmse * ci_multiplier)]
        forecast_values_list = [float(v) for v in forecast_y.values]
        
        # 6. Show last 90 days of historical + forecast (not full 2 years)
        df.reset_index(inplace=True)
        display_days = min(90, len(df))
        hist_display = df.tail(display_days)
        hist_x = hist_display['date'].tolist()
        hist_y = [float(v) for v in hist_display['y'].tolist()]

        # 7. Build Plotly figure
        fig = go.Figure()

        # Historical line (last 90 days)
        fig.add_trace(go.Scatter(
            x=hist_x, y=hist_y,
            mode='lines', name='Historical Revenue',
            line=dict(color='#111111', width=2),
            hovertemplate='%{x|%b %d, %Y}<br><b>$%{y:,.0f}</b><extra>Actual</extra>'
        ))

        # Forecast line
        fig.add_trace(go.Scatter(
            x=future_dates.tolist(), y=forecast_values_list,
            mode='lines', name=f'Forecast ({model_name})',
            line=dict(color='#E63B2E', width=3, dash='dot'),
            hovertemplate='%{x|%b %d, %Y}<br><b>$%{y:,.0f}</b><extra>Forecast</extra>'
        ))

        # Confidence band
        fig.add_trace(go.Scatter(
            x=list(future_dates) + list(future_dates[::-1]),
            y=upper_band + lower_band[::-1],
            fill='toself',
            fillcolor='rgba(230, 59, 46, 0.08)',
            line=dict(color='rgba(0,0,0,0)'),
            name='95% Confidence',
            showlegend=True,
            hoverinfo='skip'
        ))

        # Annotations with key metrics
        avg_forecast = np.mean(forecast_values_list)
        avg_historical = np.mean(hist_y)
        pct_change = ((avg_forecast - avg_historical) / avg_historical * 100) if avg_historical > 0 else 0
        
        fig.update_layout(
            title=f'Revenue Forecast // 30-Day Projection ({model_name})',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="JetBrains Mono, monospace", size=10, color='#52525B'),
            title_font=dict(family="Outfit, sans-serif", size=14, color='#111111'),
            margin=dict(t=50, b=40, l=60, r=30),
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom', y=1.02,
                xanchor='right', x=1,
                font=dict(size=9)
            ),
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(
                showgrid=True, gridcolor='#F4F4F5', zeroline=False,
                tickprefix='$', tickformat=',',
            ),
            annotations=[dict(
                text=f"Avg forecast: ${avg_forecast:,.0f}/day ({pct_change:+.1f}% vs historical)",
                xref='paper', yref='paper',
                x=0, y=-0.12,
                showarrow=False,
                font=dict(size=9, color='#9CA3AF', family='JetBrains Mono, monospace')
            )]
        )

        return pio.to_html(fig, full_html=False, include_plotlyjs=False)
