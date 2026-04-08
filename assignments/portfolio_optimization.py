#!/usr/bin/env python3
"""
Portfolio Optimization: 130/30 Strategy with Ledoit-Wolf Shrinkage
Streamlined implementation using pandas method chaining and plotnine
"""

import pandas as pd
import numpy as np
import cvxpy as cp
from plotnine import *
import warnings
warnings.filterwarnings('ignore')
# Configuration
TICKERS = ["MSFT", "AAPL", "IBM", "ORCL", "JPM", "BAC", "WFC", "AXP",
           "JNJ", "PFE", "MRK", "UNH", "HD", "MCD", "DIS", "NKE", 
           "PG", "KO", "PEP", "WMT", "XOM", "CVX", "COP", "GE", 
           "CAT", "BA", "DD", "NEE", "GLD", "USO"]

def load_returns(csv_path):
    """Load returns data from CSV file"""
    return (pd.read_csv(csv_path)
            .assign(date=lambda x: pd.to_datetime(x.date))
            .set_index('date')
            .dropna())

def calc_lw_cov(returns):
    """Ledoit-Wolf covariance shrinkage"""
    X = returns.values
    n, p = X.shape
    X_centered = X - X.mean(axis=0)
    
    # Sample covariance
    S = np.cov(X, rowvar=False)
    
    # Shrinkage calculations
    beta_sum = sum(np.sum((np.outer(row, row) - S)**2) for row in X_centered)
    beta_sq = beta_sum / n**2
    
    # Target: scaled identity
    mu = np.mean(np.diag(S))
    F = np.eye(p) * mu
    
    # Shrinkage intensity
    delta_sq = np.sum((S - F)**2)
    lambda_shrink = min(1, beta_sq / delta_sq)
    
    return lambda_shrink * F + (1 - lambda_shrink) * S

def optimize_portfolio(cov_matrix, mu=None, portfolio_type="gmv", rf=0.035/12):
    """130/30 portfolio optimization"""
    p = cov_matrix.shape[0]
    w_long = cp.Variable(p, nonneg=True)
    w_short = cp.Variable(p, nonneg=True)
    w = w_long - w_short
    
    constraints = [cp.sum(w_long) == 1.3, cp.sum(w_short) == 0.3]
    
    if portfolio_type == "gmv":
        objective = cp.Minimize(cp.quad_form(w, cov_matrix))
    else:  # orp
        excess_ret = np.array(mu) - rf
        objective = cp.Maximize(excess_ret.T @ w - 0.001 * cp.quad_form(w, cov_matrix))
    
    cp.Problem(objective, constraints).solve(verbose=False)
    return w.value

def rolling_backtest(returns, window=60, rf=0.035/12):
    """Execute rolling window backtest"""
    results = []
    
    for i in range(window, len(returns)):
        train = returns.iloc[i-window:i]
        mu = train.mean().values
        cov = calc_lw_cov(train)
        
        gmv_w = optimize_portfolio(cov, portfolio_type="gmv")
        orp_w = optimize_portfolio(cov, mu, "orp", rf)
        
        if i < len(returns) - 1:
            next_ret = returns.iloc[i+1].values
            results.append({
                'date': returns.index[i],
                'gmv_return': np.dot(gmv_w, next_ret),
                'orp_return': np.dot(orp_w, next_ret),
                'gmv_weights': gmv_w,
                'orp_weights': orp_w
            })
    
    return pd.DataFrame(results)

def analyze_performance(backtest_df):
    """Calculate performance metrics"""
    perf = (backtest_df
            .assign(
                gmv_cumret=lambda x: (1 + x.gmv_return).cumprod(),
                orp_cumret=lambda x: (1 + x.orp_return).cumprod()
            ))
    
    metrics = []
    for port in ['gmv', 'orp']:
        ret_col = f'{port}_return'
        cum_col = f'{port}_cumret'
        
        total_ret = perf[cum_col].iloc[-1] - 1
        ann_ret = perf[cum_col].iloc[-1] ** (12/len(perf)) - 1
        vol = perf[ret_col].std() * np.sqrt(12)
        sharpe = (perf[ret_col].mean() * np.sqrt(12)) / vol
        
        metrics.append({
            'portfolio': port,
            'total_return': total_ret,
            'annualized_return': ann_ret,
            'volatility': vol,
            'sharpe': sharpe
        })
    
    return pd.DataFrame(metrics), perf

def create_weights_df(backtest_df):
    """Convert weight arrays to long format"""
    weights_data = []
    asset_names = [f'asset_{i}' for i in range(len(backtest_df.iloc[0].gmv_weights))]
    
    for _, row in backtest_df.iterrows():
        for i, asset in enumerate(asset_names):
            weights_data.extend([
                {'date': row.date, 'portfolio': 'gmv', 'asset': asset, 'weight': row.gmv_weights[i]},
                {'date': row.date, 'portfolio': 'orp', 'asset': asset, 'weight': row.orp_weights[i]}
            ])
    
    return pd.DataFrame(weights_data)

def plot_results(perf_df, weights_df):
    """Create visualizations using plotnine"""
    
    # Cumulative returns
    cum_plot = (perf_df
                .melt(id_vars='date', value_vars=['gmv_cumret', 'orp_cumret'],
                      var_name='portfolio', value_name='cumulative_return')
                .pipe(lambda x: 
                    ggplot(x, aes('date', 'cumulative_return', color='portfolio')) +
                    geom_line(size=1) +
                    labs(title="Cumulative Returns (130/30 Strategy)",
                         x="Date", y="Cumulative Return") +
                    theme_minimal() +
                    scale_color_discrete(name="Portfolio"))
               )
    
    # Exposure analysis
    exposure_df = (weights_df
                   .assign(
                       long_weight=lambda x: np.maximum(x.weight, 0),
                       short_weight=lambda x: np.maximum(-x.weight, 0)
                   )
                   .groupby(['portfolio', 'date'])
                   .agg({
                       'long_weight': 'sum',
                       'short_weight': 'sum',
                       'weight': ['sum', lambda x: np.abs(x).sum()]
                   })
                   .reset_index())
    
    exposure_df.columns = ['portfolio', 'date', 'total_long', 'total_short', 'net_exposure', 'gross_exposure']
    
    exposure_plot = (exposure_df
                     .query("portfolio == 'gmv'")
                     .melt(id_vars=['date'], value_vars=['total_long', 'total_short'],
                           var_name='exposure_type', value_name='exposure')
                     .pipe(lambda x:
                         ggplot(x, aes('date', 'exposure', color='exposure_type')) +
                         geom_line(size=1) +
                         geom_hline(yintercept=1.3, linetype='dashed', alpha=0.5) +
                         geom_hline(yintercept=0.3, linetype='dashed', alpha=0.5) +
                         labs(title="Portfolio Exposures (GMV)",
                              x="Date", y="Exposure") +
                         theme_minimal()))
    
    return cum_plot, exposure_plot, exposure_df

def main(csv_path="returns_data.csv"):
    """Execute complete analysis"""
    print("Starting Portfolio Optimization Analysis...")
    
    # Load data
    print(f"Loading returns from {csv_path}...")
    returns = load_returns(csv_path)
    print(f"Data: {returns.index[0]:%Y-%m} to {returns.index[-1]:%Y-%m}")
    print(f"Assets: {returns.shape[1]}, Observations: {returns.shape[0]}")
    
    # Run backtest
    print("Executing rolling backtest...")
    backtest_results = rolling_backtest(returns)
    
    # Analyze performance
    print("Analyzing performance...")
    metrics_df, performance_df = analyze_performance(backtest_results)
    weights_df = create_weights_df(backtest_results)
    
    # Create plots
    print("Generating visualizations...")
    cum_plot, exp_plot, exposure_df = plot_results(performance_df, weights_df)
    
    # Display results
    print("\n" + "="*60)
    print("PERFORMANCE METRICS")
    print("="*60)
    print(metrics_df.round(4).to_string(index=False))
    
    print("\n" + "="*60)
    print("EXPOSURE VERIFICATION (Average)")
    print("="*60)
    exp_summary = exposure_df.groupby('portfolio')[['total_long', 'total_short', 'net_exposure']].mean()
    print(exp_summary.round(4))
    
    print("\nSaving plots...")
    cum_plot.save("cumulative_returns.png", width=10, height=6, dpi=300)
    exp_plot.save("exposures.png", width=10, height=6, dpi=300)
    
    # Export data
    print("Exporting results...")
    performance_df.to_csv("portfolio_returns.csv", index=False)
    weights_df.to_csv("portfolio_weights.csv", index=False)
    metrics_df.to_csv("performance_metrics.csv", index=False)
    
    print("Analysis complete!")
    
    return {
        'returns': performance_df,
        'weights': weights_df,
        'metrics': metrics_df,
        'exposure': exposure_df
    }

def script_to_notebook(script_path, notebook_path):
    """Convert Python script to Jupyter notebook"""
    import json
    import re
    
    with open(script_path, 'r') as f:
        content = f.read()
    
    # Split into cells based on function definitions and main sections
    cells = []
    
    # Add markdown cell for title
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": ["# Portfolio Optimization: 130/30 Strategy\n", 
                  "Streamlined implementation using pandas and plotnine\n"]
    })
    
    # Split code into logical sections
    sections = re.split(r'\n(?=def |if __name__|import)', content)
    
    for section in sections:
        if section.strip():
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [line + "\n" for line in section.split("\n")]
            })
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.9.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    with open(notebook_path, 'w') as f:
        json.dump(notebook, f, indent=2)
    
    print(f"Notebook created: {notebook_path}")

if __name__ == "__main__":
    results = main("returns_data.csv")  # Specify your CSV file path here
    
    # Convert to notebook
    script_to_notebook(__file__, "portfolio_optimization.ipynb")

def calc_lw_cov(returns):
    """Ledoit-Wolf covariance shrinkage"""
    X = returns.values
    n, p = X.shape
    X_centered = X - X.mean(axis=0)
    
    # Sample covariance
    S = np.cov(X, rowvar=False)
    
    # Shrinkage calculations
    beta_sum = sum(np.sum((np.outer(row, row) - S)**2) for row in X_centered)
    beta_sq = beta_sum / n**2
    
    # Target: scaled identity
    mu = np.mean(np.diag(S))
    F = np.eye(p) * mu
    
    # Shrinkage intensity
    delta_sq = np.sum((S - F)**2)
    lambda_shrink = min(1, beta_sq / delta_sq)
    
    return lambda_shrink * F + (1 - lambda_shrink) * S

def optimize_portfolio(cov_matrix, mu=None, portfolio_type="gmv", rf=0.035/12):
    """130/30 portfolio optimization"""
    p = cov_matrix.shape[0]
    w_long = cp.Variable(p, nonneg=True)
    w_short = cp.Variable(p, nonneg=True)
    w = w_long - w_short
    
    constraints = [cp.sum(w_long) == 1.3, cp.sum(w_short) == 0.3]
    
    if portfolio_type == "gmv":
        objective = cp.Minimize(cp.quad_form(w, cov_matrix))
    else:  # orp
        excess_ret = np.array(mu) - rf
        objective = cp.Maximize(excess_ret.T @ w - 0.001 * cp.quad_form(w, cov_matrix))
    
    cp.Problem(objective, constraints).solve(verbose=False)
    return w.value

def rolling_backtest(returns, window=60, rf=0.035/12):
    """Execute rolling window backtest"""
    results = []
    
    for i in range(window, len(returns)):
        train = returns.iloc[i-window:i]
        mu = train.mean().values
        cov = calc_lw_cov(train)
        
        gmv_w = optimize_portfolio(cov, portfolio_type="gmv")
        orp_w = optimize_portfolio(cov, mu, "orp", rf)
        
        if i < len(returns) - 1:
            next_ret = returns.iloc[i+1].values
            results.append({
                'date': returns.index[i],
                'gmv_return': np.dot(gmv_w, next_ret),
                'orp_return': np.dot(orp_w, next_ret),
                'gmv_weights': gmv_w,
                'orp_weights': orp_w
            })
    
    return pd.DataFrame(results)

def analyze_performance(backtest_df):
    """Calculate performance metrics"""
    perf = (backtest_df
            .assign(
                gmv_cumret=lambda x: (1 + x.gmv_return).cumprod(),
                orp_cumret=lambda x: (1 + x.orp_return).cumprod()
            ))
    
    metrics = []
    for port in ['gmv', 'orp']:
        ret_col = f'{port}_return'
        cum_col = f'{port}_cumret'
        
        total_ret = perf[cum_col].iloc[-1] - 1
        ann_ret = perf[cum_col].iloc[-1] ** (12/len(perf)) - 1
        vol = perf[ret_col].std() * np.sqrt(12)
        sharpe = (perf[ret_col].mean() * np.sqrt(12)) / vol
        
        metrics.append({
            'portfolio': port,
            'total_return': total_ret,
            'annualized_return': ann_ret,
            'volatility': vol,
            'sharpe': sharpe
        })
    
    return pd.DataFrame(metrics), perf

def create_weights_df(backtest_df, tickers):
    """Convert weight arrays to long format"""
    weights_data = []
    
    for _, row in backtest_df.iterrows():
        for i, ticker in enumerate(tickers):
            weights_data.extend([
                {'date': row.date, 'portfolio': 'gmv', 'asset': ticker, 'weight': row.gmv_weights[i]},
                {'date': row.date, 'portfolio': 'orp', 'asset': ticker, 'weight': row.orp_weights[i]}
            ])
    
    return pd.DataFrame(weights_data)

def plot_results(perf_df, weights_df):
    """Create visualizations using plotnine"""
    
    # Cumulative returns
    cum_plot = (perf_df
                .melt(id_vars='date', value_vars=['gmv_cumret', 'orp_cumret'],
                      var_name='portfolio', value_name='cumulative_return')
                .pipe(lambda x: 
                    ggplot(x, aes('date', 'cumulative_return', color='portfolio')) +
                    geom_line(size=1) +
                    labs(title="Cumulative Returns (130/30 Strategy)",
                         x="Date", y="Cumulative Return") +
                    theme_minimal() +
                    scale_color_discrete(name="Portfolio"))
               )
    
    # Exposure analysis
    exposure_df = (weights_df
                   .assign(
                       long_weight=lambda x: np.maximum(x.weight, 0),
                       short_weight=lambda x: np.maximum(-x.weight, 0)
                   )
                   .groupby(['portfolio', 'date'])
                   .agg({
                       'long_weight': 'sum',
                       'short_weight': 'sum',
                       'weight': ['sum', lambda x: np.abs(x).sum()]
                   })
                   .reset_index())
    
    exposure_df.columns = ['portfolio', 'date', 'total_long', 'total_short', 'net_exposure', 'gross_exposure']
    
    exposure_plot = (exposure_df
                     .query("portfolio == 'gmv'")
                     .melt(id_vars=['date'], value_vars=['total_long', 'total_short'],
                           var_name='exposure_type', value_name='exposure')
                     .pipe(lambda x:
                         ggplot(x, aes('date', 'exposure', color='exposure_type')) +
                         geom_line(size=1) +
                         geom_hline(yintercept=1.3, linetype='dashed', alpha=0.5) +
                         geom_hline(yintercept=0.3, linetype='dashed', alpha=0.5) +
                         labs(title="Portfolio Exposures (GMV)",
                              x="Date", y="Exposure") +
                         theme_minimal()))
    
    return cum_plot, exposure_plot, exposure_df

def main():
    """Execute complete analysis"""
    print("Starting Portfolio Optimization Analysis...")
    
    # Load data
    print(f"Loading data for {len(TICKERS)} assets...")
    returns = load_returns('returns_data.csv')
    print(f"Data: {returns.index[0]:%Y-%m} to {returns.index[-1]:%Y-%m}")
    
    # Run backtest
    print("Executing rolling backtest...")
    backtest_results = rolling_backtest(returns)
    
    # Analyze performance
    print("Analyzing performance...")
    metrics_df, performance_df = analyze_performance(backtest_results)
    weights_df = create_weights_df(backtest_results, TICKERS)
    
    # Create plots
    print("Generating visualizations...")
    cum_plot, exp_plot, exposure_df = plot_results(performance_df, weights_df)
    
    # Display results
    print("\n" + "="*60)
    print("PERFORMANCE METRICS")
    print("="*60)
    print(metrics_df.round(4).to_string(index=False))
    
    print("\n" + "="*60)
    print("EXPOSURE VERIFICATION (Average)")
    print("="*60)
    exp_summary = exposure_df.groupby('portfolio')[['total_long', 'total_short', 'net_exposure']].mean()
    print(exp_summary.round(4))
    
    print("\nSaving plots...")
    cum_plot.save("cumulative_returns.png", width=10, height=6, dpi=300)
    exp_plot.save("exposures.png", width=10, height=6, dpi=300)
    
    # Export data
    print("Exporting results...")
    performance_df.to_csv("portfolio_returns.csv", index=False)
    weights_df.to_csv("portfolio_weights.csv", index=False)
    metrics_df.to_csv("performance_metrics.csv", index=False)
    
    print("Analysis complete!")
    
    return {
        'returns': performance_df,
        'weights': weights_df,
        'metrics': metrics_df,
        'exposure': exposure_df
    }

def script_to_notebook(script_path, notebook_path):
    """Convert Python script to Jupyter notebook"""
    import json
    import re
    
    with open(script_path, 'r') as f:
        content = f.read()
    
    # Split into cells based on function definitions and main sections
    cells = []
    
    # Add markdown cell for title
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": ["# Portfolio Optimization: 130/30 Strategy\\n", 
                  "Streamlined implementation using pandas and plotnine"]
    })
    
    # Split code into logical sections
    sections = re.split(r'\n(?=def |if __name__|# |import)', content)
    
    current_code = []
    for section in sections:
        if section.strip():
            if section.startswith('#') or section.startswith('"""'):
                # Markdown cell
                if current_code:
                    cells.append({
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": [line + "\\n" for line in "\\n".join(current_code).split("\\n")]
                    })
                    current_code = []
                
                cells.append({
                    "cell_type": "markdown", 
                    "metadata": {},
                    "source": [f"## {section.strip('# \"')}\\n"]
                })
            else:
                current_code.append(section)
    
    # Add final code cell
    if current_code:
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [line + "\\n" for line in "\\n".join(current_code).split("\\n")]
        })
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.9.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    with open(notebook_path, 'w') as f:
        json.dump(notebook, f, indent=2)
    
    print(f"Notebook created: {notebook_path}")

if __name__ == "__main__":
    results = main()
    
    # Convert to notebook
    script_to_notebook(__file__, "portfolio_optimization.ipynb")
