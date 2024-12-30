import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import requests
import pandas as pd
import numpy as np
from spark_consumer import calculate_technical_indicators, calculate_metrics

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "Crypto Trading Dashboard"

# Layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Crypto Trading Dashboard", style={'textAlign': 'center', 'color': '#2c3e50'}),
        dcc.Dropdown(
            id='crypto-dropdown',
            options=[
                {'label': 'Bitcoin (BTC)', 'value': 'BTCUSDT'},
                {'label': 'Ethereum (ETH)', 'value': 'ETHUSDT'},
            ],
            value='BTCUSDT',
            style={'width': '200px', 'margin': '0 auto'}
        ),
    ], style={'marginBottom': '30px'}),
    
    # Metrics Section in Cards
    html.Div(id='metrics-info', className='metrics-container', style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': '30px'}),
    
    # Price and Prediction Section
    html.Div([
        dcc.Graph(id='price-prediction-chart'),
    ], className='chart-container'),

    # Technical Analysis Section
    html.Div([
        html.H2("Technical Analysis", style={'textAlign': 'center', 'color': '#2c3e50'}),
        html.Div([
            dcc.Graph(id='rsi-chart', style={'width': '33%'}),
            dcc.Graph(id='macd-chart', style={'width': '33%'}),
            dcc.Graph(id='accuracy-trend', style={'width': '33%'}),
        ], style={'display': 'flex'}),
    ]),

    # Bollinger Bands Section
    html.Div([
        dcc.Graph(id='bb-chart'),
    ]),

    # Update interval
    dcc.Interval(
        id='update-interval',
        interval=60000,  # 1 minute
        n_intervals=0
    )
], style={'padding': '20px'})

# Helper Functions
def fetch_data(symbol):
    """Fetch data from the Flask API."""
    response = requests.get(f'http://localhost:5000/stream')
    data = response.json() if response.status_code == 200 else []

    df = pd.DataFrame(data)
    if symbol in df['symbol'].values:
        df = df[df['symbol'] == symbol]
    return df

# Callback for updating all components
@app.callback(
    [Output('metrics-info', 'children'),
     Output('price-prediction-chart', 'figure'),
     Output('rsi-chart', 'figure'),
     Output('macd-chart', 'figure'),
     Output('accuracy-trend', 'figure'),
     Output('bb-chart', 'figure')],
    [Input('update-interval', 'n_intervals'),
     Input('crypto-dropdown', 'value')]
)
def update_dashboard(n_intervals, symbol):
    df = fetch_data(symbol)
    
    # Assuming `calculate_technical_indicators` and `calculate_metrics` functions are used to compute technical analysis metrics
    df = calculate_technical_indicators(df)
    metrics = calculate_metrics(df)

    metrics_info = generate_metrics_info(metrics)
    price_prediction_chart = generate_price_prediction_chart(df)
    rsi_chart = generate_rsi_chart(df)
    macd_chart = generate_macd_chart(df)
    accuracy_trend = generate_accuracy_trend(df)
    bb_chart = generate_bb_chart(df)

    return (metrics_info, price_prediction_chart, rsi_chart, macd_chart, accuracy_trend, bb_chart)

def generate_metrics_info(metrics):
    """Generate metrics display in card format with shadows and conditional colors."""
    change_color = '#e74c3c' if metrics['price_change'] < 0 else '#27ae60'
    
    return [
        html.Div([
            html.H4(f"Change: {metrics['price_change']:.2f}%", style={'margin': '5px', 'color': change_color}),
        ], style={'border': '1px solid #2c3e50', 'borderRadius': '5px', 'padding': '10px', 'width': '150px', 'boxShadow': '2px 2px 5px rgba(0,0,0,0.3)', 'textAlign': 'center'}),
        
        html.Div([
            html.H4(f"Lowest Price: ${metrics['lowest_price']:.2f}", style={'margin': '5px'}),
        ], style={'border': '1px solid #2c3e50', 'borderRadius': '5px', 'padding': '10px', 'width': '150px', 'boxShadow': '2px 2px 5px rgba(0,0,0,0.3)', 'textAlign': 'center'}),
        
        html.Div([
            html.H4(f"Highest Price: ${metrics['highest_price']:.2f}", style={'margin': '5px'}),
        ], style={'border': '1px solid #2c3e50', 'borderRadius': '5px', 'padding': '10px', 'width': '150px', 'boxShadow': '2px 2px 5px rgba(0,0,0,0.3)', 'textAlign': 'center'}),
        
        html.Div([
            html.H4(f"Volume: ${metrics['volume']:.0f}", style={'margin': '5px'}),
        ], style={'border': '1px solid #2c3e50', 'borderRadius': '5px', 'padding': '10px', 'width': '150px', 'boxShadow': '2px 2px 5px rgba(0,0,0,0.3)', 'textAlign': 'center'}),
    ]

def generate_price_prediction_chart(df):
    """Generate price prediction chart."""
    return {
        'data': [
            go.Scatter(
                x=df['timestamp'],
                y=df['price'],
                name='Actual Price',
                line={'color': '#2980b9'},
                mode='lines+markers'  # Add markers to actual price
            ),
            go.Scatter(
                x=df['timestamp'],
                y=df['predicted_price'],
                name='Predicted Price',
                line={'color': '#e74c3c', 'dash': 'dash', 'width': 2},
                mode='lines+markers',  # Add markers to predicted price
                opacity=0.8  # Slightly transparent
            )
        ],
        'layout': {
            'title': 'Actual vs Predicted Price',
            'xaxis': {'title': 'Time'},
            'yaxis': {
                'title': 'Price (USD)',
                'range': [min(df['predicted_price'].min(), df['price'].min()) - 10, 
                           max(df['predicted_price'].max(), df['price'].max()) + 10]  # Set Y-axis range
            },
            'template': 'plotly_white'
        }
    }

def generate_rsi_chart(df):
    """Generate RSI chart."""
    return {
        'data': [
            go.Scatter(x=df['timestamp'], y=df['RSI'], name='RSI', line={'color': '#8e44ad'}),
            go.Scatter(x=df['timestamp'], y=[70] * len(df), name='Overbought', line={'color': '#e74c3c', 'dash': 'dash'}),
            go.Scatter(x=df['timestamp'], y=[30] * len(df), name='Oversold', line={'color': '#27ae60', 'dash': 'dash'})
        ],
        'layout': {
            'title': 'RSI',
            'xaxis': {'title': 'Time'},
            'yaxis': {'title': 'RSI', 'range': [0, 100]},
            'template': 'plotly_white'
        }
    }

def generate_macd_chart(df):
    """Generate MACD chart."""
    return {
        'data': [
            go.Bar(x=df['timestamp'], y=df['MACD_hist'], name='MACD Histogram'),
            go.Scatter(x=df['timestamp'], y=df['MACD'], name='MACD', line={'color': '#2980b9'}),
            go.Scatter(x=df['timestamp'], y=df['MACD_signal'], name='Signal', line={'color': '#e74c3c'})
        ],
        'layout': {
            'title': 'MACD',
            'xaxis': {'title': 'Time'},
            'yaxis': {'title': 'Value'},
            'template': 'plotly_white'
        }
    }

def generate_accuracy_trend(df):
    """Generate accuracy trend chart."""
    errors = abs(df['price'] - df['predicted_price']) / df['price'] * 100
    return {
        'data': [
            go.Scatter(x=df['timestamp'], y=errors, name='Prediction Error', line={'color': '#f39c12'})
        ],
        'layout': {
            'title': 'Prediction Error (%)',
            'xaxis': {'title': 'Time'},
            'yaxis': {'title': 'Error (%)', 'range': [0, 100]},
            'template': 'plotly_white'
        }
    }

def generate_bb_chart(df):
    """Generate Bollinger Bands chart."""
    return {
        'data': [
            go.Scatter(x=df['timestamp'], y=df['BB_upper'], name='Upper Band', line={'color': '#e74c3c', 'dash': 'dot'}),
            go.Scatter(x=df['timestamp'], y=df['BB_lower'], name='Lower Band', line={'color': '#27ae60', 'dash': 'dot'}),
            go.Scatter(x=df['timestamp'], y=df['price'], name='Price', line={'color': '#2980b9'})
        ],
        'layout': {
            'title': 'Bollinger Bands',
            'xaxis': {'title': 'Time'},
            'yaxis': {'title': 'Price (USD)'},
            'template': 'plotly_white'
        }
    }

if __name__ == '__main__':
    app.run_server(debug=True)

