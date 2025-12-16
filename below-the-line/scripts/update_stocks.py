#!/usr/bin/env python3
"""
Below The Line - Weekly Stock Data Pipeline

Fetches weekly price data from Alpha Vantage, calculates:
- 200-week moving average
- Distance from 200WMA (%)
- Week-over-week directional change
- 14-week RSI
- Historical touches of the 200WMA

Run weekly on Saturday to capture Friday close data.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import pandas as pd
import requests

# Configuration
API_KEY = os.environ.get('ALPHA_VANTAGE_KEY')
OUTPUT_DIR = Path(__file__).parent.parent / 'assets' / 'data'
COMPANIES_FILE = OUTPUT_DIR / 'companies.json'
RATE_LIMIT_DELAY = 12  # seconds between calls (5 calls/min limit)

# Load company metadata
def load_company_metadata():
    """Load company names, sectors, and IR URLs from reference file."""
    if COMPANIES_FILE.exists():
        with open(COMPANIES_FILE) as f:
            return json.load(f)
    return {}

# Stock universe - Berkshire holdings + S&P 500 + Speculative/Growth
STOCK_UNIVERSE = [
    # === SPECULATIVE / HIGH-INTEREST ===
    'UUUU',   # Energy Fuels (uranium)
    'RKLB',   # Rocket Lab
    'ASTS',   # AST SpaceMobile
    'LUNR',   # Intuitive Machines
    'RDW',    # Redwire
    'IONQ',   # IonQ (quantum computing)
    'RGTI',   # Rigetti Computing
    'SMR',    # NuScale Power
    'LEU',    # Centrus Energy
    'CCJ',    # Cameco (uranium)
    'DNN',    # Denison Mines
    'PLTR',   # Palantir
    'SOFI',   # SoFi Technologies
    'HOOD',   # Robinhood
    'AFRM',   # Affirm
    'UPST',   # Upstart
    'PATH',   # UiPath
    'S',      # SentinelOne
    'CRWD',   # CrowdStrike
    'NET',    # Cloudflare
    'DDOG',   # Datadog
    'SNOW',   # Snowflake
    'MDB',    # MongoDB
    'COIN',   # Coinbase
    'MSTR',   # MicroStrategy
    'ARKK',   # ARK Innovation ETF
    
    # === BERKSHIRE HATHAWAY HOLDINGS ===
    'AAPL', 'BAC', 'AXP', 'KO', 'CVX', 'OXY', 'KHC', 'MCO', 'CB', 'DVA',
    'C', 'KR', 'VRSN', 'V', 'MA', 'AMZN', 'NU', 'AON', 'COF', 'ALLY',
    
    # === MAJOR S&P 500 ===
    'MSFT', 'GOOGL', 'GOOG', 'META', 'NVDA', 'TSLA', 'BRK-B', 'JPM', 'JNJ',
    'UNH', 'XOM', 'PG', 'HD', 'MRK', 'ABBV', 'PEP', 'COST', 'AVGO', 'TMO',
    'MCD', 'WMT', 'CSCO', 'ACN', 'LIN', 'ABT', 'DHR', 'NEE', 'ADBE', 'NKE',
    'TXN', 'PM', 'RTX', 'ORCL', 'HON', 'UNP', 'IBM', 'QCOM', 'SPGI', 'CAT',
    'GE', 'AMGN', 'INTU', 'AMAT', 'ISRG', 'BKNG', 'PLD', 'MDLZ', 'GS', 'BLK',
    'INTC', 'AMD', 'CRM', 'NOW', 'UBER', 'SQ', 'SHOP', 'ZS', 'PANW', 'FTNT',

    # === REDDIT POPULAR / MEME STOCKS ===
    'TSM',    # Taiwan Semiconductor
    'GME',    # GameStop
    'AMC',    # AMC Entertainment
    'RIVN',   # Rivian Automotive
    'LCID',   # Lucid Group
    'PYPL',   # PayPal
    'DIS',    # Disney
    'BABA',   # Alibaba
    'ARM',    # Arm Holdings

    # === VALUE INVESTING (/r/valueinvesting) ===
    'GILD',   # Gilead Sciences
    'BMY',    # Bristol-Myers Squibb
    'VZ',     # Verizon
    'T',      # AT&T
    'WBA',    # Walgreens Boots Alliance
    'PFE',    # Pfizer
    'F',      # Ford Motor
    'GM',     # General Motors
    'O',      # Realty Income
    'BTI',    # British American Tobacco
    'MO',     # Altria
    'LMT',    # Lockheed Martin
    'TGT',    # Target
    'NVO',    # Novo Nordisk
    'LLY',    # Eli Lilly
]


def fetch_weekly_data(symbol: str) -> Optional[pd.DataFrame]:
    """
    Fetch weekly adjusted price data from Alpha Vantage.
    
    Uses TIME_SERIES_WEEKLY_ADJUSTED which returns one row per week
    (Friday close), so rolling(200) = 200 weeks.
    
    Returns DataFrame with columns: close, adjusted_close, volume
    """
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_WEEKLY_ADJUSTED',
        'symbol': symbol,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        # Check for API errors
        if 'Error Message' in data:
            print(f"  ✗ API error for {symbol}: {data['Error Message']}")
            return None
        if 'Note' in data:
            print(f"  ✗ Rate limit hit: {data['Note']}")
            return None
        
        weekly_data = data.get('Weekly Adjusted Time Series', {})
        if not weekly_data:
            print(f"  ✗ No data returned for {symbol}")
            return None
        
        df = pd.DataFrame.from_dict(weekly_data, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        # Rename columns for clarity
        df = df.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. adjusted close': 'adjusted_close',
            '6. volume': 'volume',
            '7. dividend amount': 'dividend'
        })
        
        # Convert to numeric
        for col in ['open', 'high', 'low', 'close', 'adjusted_close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        print(f"  ✗ Error fetching {symbol}: {e}")
        return None


def calculate_rsi(prices: pd.Series, periods: int = 14) -> pd.Series:
    """
    Calculate RSI (Relative Strength Index).
    
    14-week RSI on weekly data:
    - < 30: Oversold (potential buying opportunity)
    - < 20: Extremely oversold
    - > 70: Overbought
    """
    delta = prices.diff()
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=periods).mean()
    avg_loss = loss.rolling(window=periods).mean()
    
    # Avoid division by zero
    rs = avg_gain / avg_loss.replace(0, float('inf'))
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def find_historical_touches(df: pd.DataFrame) -> List[dict]:
    """
    Find all periods where stock crossed below 200WMA.
    
    Returns list of touch events with:
    - Start date
    - Duration (weeks below)
    - Maximum depth below line
    - Return 1 year after crossing back above
    """
    touches = []
    
    # Identify cross-below events
    df['below_line'] = df['adjusted_close'] < df['WMA_200']
    df['cross_below'] = (df['below_line']) & (~df['below_line'].shift(1).fillna(False))
    
    cross_dates = df[df['cross_below']].index.tolist()
    
    for cross_date in cross_dates:
        # Find when it crossed back above
        subsequent = df[df.index > cross_date]
        cross_above = subsequent[~subsequent['below_line']]
        
        if len(cross_above) > 0:
            recovery_date = cross_above.index[0]
            
            # Calculate weeks below
            period_below = df[(df.index >= cross_date) & (df.index < recovery_date)]
            weeks_below = len(period_below)
            
            # Calculate max depth below line during this period
            max_depth = period_below['pct_from_wma'].min()
            
            # Calculate 1-year return from recovery date
            one_year_later = recovery_date + pd.DateOffset(weeks=52)
            if one_year_later <= df.index[-1]:
                recovery_price = df.loc[recovery_date, 'adjusted_close']
                future_data = df[df.index >= one_year_later]
                if len(future_data) > 0:
                    future_price = future_data.iloc[0]['adjusted_close']
                    one_year_return = ((future_price - recovery_price) / recovery_price) * 100
                else:
                    one_year_return = None
            else:
                one_year_return = None
            
            touches.append({
                'date': cross_date.strftime('%b %Y'),
                'date_iso': cross_date.strftime('%Y-%m-%d'),
                'weeks_below': int(weeks_below),
                'max_depth': round(float(max_depth), 1),
                'return_1yr': round(float(one_year_return), 1) if one_year_return is not None else None
            })
        else:
            # Currently still below the line
            period_below = df[df.index >= cross_date]
            weeks_below = len(period_below)
            max_depth = period_below['pct_from_wma'].min()
            
            touches.append({
                'date': cross_date.strftime('%b %Y'),
                'date_iso': cross_date.strftime('%Y-%m-%d'),
                'weeks_below': int(weeks_below),
                'max_depth': round(float(max_depth), 1),
                'return_1yr': None,
                'ongoing': True
            })
    
    return touches


def calculate_stock_signals(symbol: str) -> Optional[dict]:
    """
    Calculate all signals for a stock.
    
    Returns dict with:
    - symbol, close, wma_200
    - pct_from_wma (distance from 200WMA)
    - wow_change (week-over-week directional change)
    - rsi_14 (14-week RSI)
    - below_line (boolean)
    - approaching (boolean - moving toward the line)
    - historical_touches (list of past touches)
    """
    print(f"  Processing {symbol}...")
    
    df = fetch_weekly_data(symbol)
    if df is None:
        return None
    
    if len(df) < 200:
        print(f"  ✗ {symbol}: Only {len(df)} weeks of data (need 200+)")
        return None
    
    # === 200-WEEK MOVING AVERAGE ===
    df['WMA_200'] = df['adjusted_close'].rolling(window=200).mean()
    
    # === DISTANCE FROM 200WMA ===
    df['pct_from_wma'] = ((df['adjusted_close'] - df['WMA_200']) / df['WMA_200']) * 100
    
    # === WEEK-OVER-WEEK DIRECTIONAL CHANGE ===
    # Negative = moving toward line (approaching)
    # Positive = moving away from line
    df['wow_change'] = df['pct_from_wma'] - df['pct_from_wma'].shift(1)
    
    # === 14-WEEK RSI ===
    df['RSI_14'] = calculate_rsi(df['adjusted_close'], periods=14)
    
    # === HISTORICAL TOUCHES ===
    # Need to calculate after WMA and pct_from_wma are ready
    df_complete = df.dropna(subset=['WMA_200'])
    historical_touches = find_historical_touches(df_complete.copy())
    
    # Get latest values
    latest = df_complete.iloc[-1]
    previous = df_complete.iloc[-2] if len(df_complete) > 1 else latest
    
    # Calculate buy threshold (the 200WMA value)
    buy_threshold = latest['WMA_200']
    
    # Determine signal color zone
    pct = latest['pct_from_wma']
    if pct <= -10:
        zone = 'extreme_value'
    elif pct <= -5:
        zone = 'deep_value'
    elif pct <= 0:
        zone = 'below_line'
    elif pct <= 5:
        zone = 'at_doorstep'
    elif pct <= 10:
        zone = 'getting_close'
    elif pct <= 15:
        zone = 'approaching'
    else:
        zone = 'above'
    
    # Calculate average return from historical touches
    returns = [t['return_1yr'] for t in historical_touches if t.get('return_1yr') is not None]
    avg_return = round(sum(returns) / len(returns), 1) if returns else None
    avg_weeks = round(sum(t['weeks_below'] for t in historical_touches) / len(historical_touches), 1) if historical_touches else None
    
    result = {
        'symbol': symbol,
        'close': round(float(latest['adjusted_close']), 2),
        'wma_200': round(float(latest['WMA_200']), 2),
        'buy_threshold': round(float(buy_threshold), 2),
        'pct_from_wma': round(float(latest['pct_from_wma']), 2),
        'wow_change': round(float(latest['wow_change']), 2),
        'rsi_14': round(float(latest['RSI_14']), 1),
        'below_line': bool(latest['adjusted_close'] < latest['WMA_200']),
        'approaching': float(latest['wow_change']) < 0,
        'zone': zone,
        'historical_touches': historical_touches,
        'touch_count': len(historical_touches),
        'avg_return_after_touch': avg_return,
        'avg_weeks_below': avg_weeks,
        'last_updated': df_complete.index[-1].strftime('%Y-%m-%d'),
        'data_weeks': len(df_complete)
    }
    
    print(f"  ✓ {symbol}: {pct:.1f}% from WMA, RSI: {latest['RSI_14']:.0f}, Zone: {zone}")
    return result


def generate_landing_page_data(stocks: List[dict]) -> dict:
    """
    Generate summary data for the landing page.
    """
    below_line = [s for s in stocks if s['below_line']]
    approaching = [s for s in stocks if not s['below_line'] and s['zone'] in ['at_doorstep', 'getting_close', 'approaching']]
    oversold = [s for s in stocks if s['rsi_14'] < 30]
    
    # Sort by closest to line
    approaching_sorted = sorted(approaching, key=lambda x: x['pct_from_wma'])
    below_sorted = sorted(below_line, key=lambda x: x['pct_from_wma'])
    
    return {
        'total_stocks': len(stocks),
        'below_line_count': len(below_line),
        'approaching_count': len(approaching),
        'oversold_count': len(oversold),
        'below_line_stocks': below_sorted,
        'approaching_stocks': approaching_sorted[:10],  # Top 10 nearest
        'oversold_stocks': [s for s in stocks if s['rsi_14'] < 30],
    }


def main():
    """Main entry point for weekly data update."""
    print("=" * 60)
    print("Below The Line - Weekly Data Update")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    if not API_KEY:
        print("ERROR: ALPHA_VANTAGE_KEY environment variable not set")
        return
    
    # Load company metadata
    companies = load_company_metadata()
    print(f"Loaded metadata for {len(companies)} companies")
    
    results = []
    errors = []
    
    print(f"\nProcessing {len(STOCK_UNIVERSE)} stocks...")
    print("-" * 60)
    
    for i, symbol in enumerate(STOCK_UNIVERSE):
        data = calculate_stock_signals(symbol)
        
        if data:
            # Merge company metadata
            if symbol in companies:
                data['name'] = companies[symbol].get('name', symbol)
                data['sector'] = companies[symbol].get('sector', '')
                data['ir_url'] = companies[symbol].get('ir_url', '')
            else:
                data['name'] = symbol
                data['sector'] = ''
                data['ir_url'] = ''
            
            results.append(data)
        else:
            errors.append(symbol)
        
        # Rate limiting - wait between calls
        if i < len(STOCK_UNIVERSE) - 1:
            time.sleep(RATE_LIMIT_DELAY)
    
    print("-" * 60)
    print(f"Successfully processed: {len(results)}/{len(STOCK_UNIVERSE)}")
    if errors:
        print(f"Errors: {', '.join(errors)}")
    
    # Generate output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Individual stock data
    output = {
        'stocks': results,
        'summary': generate_landing_page_data(results),
        'generated': datetime.now().isoformat(),
        'generated_readable': datetime.now().strftime('%B %d, %Y')
    }
    
    output_file = OUTPUT_DIR / 'stocks.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nOutput written to: {output_file}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == '__main__':
    main()
