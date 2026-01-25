#!/usr/bin/env python3
"""
Below The Line - Weekly Stock Data Pipeline

Fetches weekly price data from Yahoo Finance, calculates:
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
import yfinance as yf

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent / 'assets' / 'data'
COMPANIES_FILE = OUTPUT_DIR / 'companies.json'

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
    'TSM', 'GME', 'AMC', 'RIVN', 'LCID', 'PYPL', 'DIS', 'BABA', 'ARM',

    # === VALUE INVESTING CORE ===
    'GILD', 'BMY', 'VZ', 'T', 'WBA', 'PFE', 'F', 'GM', 'O', 'SCHD',

    # === NEW VALUE STOCKS - FINANCIALS (20) ===
    'WFC', 'USB', 'PNC', 'TFC', 'SCHW', 'MS', 'AIG', 'MET', 'PRU', 'ALL',
    'TRV', 'AFL', 'CME', 'ICE', 'FITB', 'KEY', 'RF', 'CFG', 'MTB', 'HBAN',

    # === NEW VALUE STOCKS - UTILITIES (15) ===
    'DUK', 'SO', 'D', 'AEP', 'XEL', 'EXC', 'SRE', 'ED', 'WEC', 'ES',
    'PPL', 'ETR', 'AES', 'CNP', 'NI',

    # === NEW VALUE STOCKS - CONSUMER STAPLES (15) ===
    'CL', 'GIS', 'SJM', 'CPB', 'CAG', 'HRL', 'MKC', 'CHD', 'CLX', 'KMB',
    'TAP', 'STZ', 'BG', 'ADM', 'K',

    # === NEW VALUE STOCKS - HEALTHCARE (10) ===
    'CVS', 'CI', 'HUM', 'CNC', 'ELV', 'MCK', 'CAH', 'VTRS', 'ZTS', 'LLY',

    # === NEW VALUE STOCKS - INDUSTRIALS (15) ===
    'MMM', 'EMR', 'ETN', 'ITW', 'PH', 'SWK', 'DOV', 'SNA', 'CMI', 'PCAR',
    'DE', 'FDX', 'UPS', 'NSC', 'CSX',

    # === NEW VALUE STOCKS - DEFENSE (4) ===
    'LMT', 'NOC', 'GD', 'BA',

    # === NEW VALUE STOCKS - ENERGY (6) ===
    'SLB', 'HAL', 'BKR', 'DVN', 'MPC', 'VLO',

    # === NEW VALUE STOCKS - REITS (10) ===
    'SPG', 'AMT', 'CCI', 'EQIX', 'DLR', 'PSA', 'AVB', 'EQR', 'WELL', 'VTR',

    # === NEW VALUE STOCKS - MATERIALS (5) ===
    'NUE', 'FCX', 'NEM', 'DOW', 'LYB',

    # === REGIONAL BANKS & ASSET MANAGERS (15) ===
    'ZION', 'CMA', 'FHN', 'EWBC', 'WAL', 'BOKF', 'FNB', 'TROW', 'IVZ', 'BEN',
    'NTRS', 'STT', 'AMG', 'SEIC', 'CBOE',

    # === HEALTHCARE - PHARMA, BIOTECH, DEVICES (15) ===
    'BIIB', 'REGN', 'VRTX', 'MRNA', 'ILMN', 'DXCM', 'IDXX', 'MTD', 'STE', 'BAX',
    'BDX', 'SYK', 'MDT', 'BSX', 'EW',

    # === INDUSTRIALS - MACHINERY, CONSTRUCTION (15) ===
    'ROK', 'AME', 'GNRC', 'IR', 'XYL', 'GWW', 'FAST', 'URI', 'PWR', 'J',
    'WAB', 'TT', 'CARR', 'OTIS', 'LHX',

    # === CONSUMER DISCRETIONARY - RETAIL, RESTAURANTS (15) ===
    'TJX', 'ROST', 'DG', 'DLTR', 'ORLY', 'AZO', 'BBY', 'LOW', 'TGT', 'SBUX',
    'CMG', 'DPZ', 'YUM', 'MAR', 'HLT',

    # === TECHNOLOGY - HARDWARE & SEMICONDUCTORS (12) ===
    'HPQ', 'HPE', 'DELL', 'WDC', 'STX', 'NTAP', 'KEYS', 'ANSS', 'CDNS', 'SNPS',
    'KLAC', 'LRCX',

    # === ENERGY - PIPELINES & E&P (10) ===
    'WMB', 'KMI', 'OKE', 'ET', 'EPD', 'PXD', 'EOG', 'COP', 'HES', 'FANG',

    # === MATERIALS - CHEMICALS, METALS (8) ===
    'APD', 'ECL', 'SHW', 'PPG', 'ALB', 'CF', 'MOS', 'BALL',

    # === COMMUNICATION SERVICES (5) ===
    'TMUS', 'CHTR', 'CMCSA', 'WBD', 'NFLX',

    # === INTERNATIONAL ADRs (5) ===
    'TM', 'SNY', 'NVS', 'UL', 'BTI',

    # === MID-CAP FINANCIALS (18) ===
    'FCNCA', 'SNV', 'ONB', 'UBSI', 'FFIN', 'GBCI', 'SBCF', 'HWC', 'SFNC',
    'WTFC', 'PNFP', 'FBK', 'IBOC', 'CADE', 'AUB', 'TCBI', 'ABCB', 'SSB',

    # === MORE HEALTHCARE - BIOTECH & SERVICES (24) ===
    'ALNY', 'SRPT', 'BMRN', 'INCY', 'EXAS', 'TECH', 'NTRA', 'RARE', 'NBIX',
    'PCVX', 'KRYS', 'INSM', 'IONS', 'UTHR', 'EXEL', 'ACAD', 'ARVN', 'FOLD',
    'HALO', 'LGND', 'MEDP', 'ICLR', 'CRL', 'WST',

    # === MORE CONSUMER DISCRETIONARY (25) ===
    'GRMN', 'DECK', 'POOL', 'WSM', 'RH', 'TSCO', 'ULTA', 'FIVE', 'BOOT', 'OLLI',
    'BURL', 'SKX', 'CROX', 'LULU', 'NVR', 'PHM', 'LEN', 'DHI', 'TOL', 'KBH',
    'MTH', 'MDC', 'PENN', 'CZR', 'WYNN',

    # === MORE INDUSTRIALS (23) ===
    'ALLE', 'AOS', 'CR', 'FICO', 'FTV', 'GGG', 'IEX', 'MIDD', 'NDSN', 'RBC',
    'RRX', 'SITE', 'TTC', 'WCC', 'WWD', 'ZWS', 'AXON', 'TDG', 'HEI', 'HEI-A',
    'BWXT', 'HII', 'LDOS',

    # === MORE TECHNOLOGY - SOFTWARE & SERVICES (23) ===
    'TEAM', 'HUBS', 'WDAY', 'VEEV', 'OKTA', 'TWLO', 'DBX', 'ZI', 'ESTC', 'CFLT',
    'GTLB', 'BILL', 'PAYC', 'PCTY', 'WK', 'APPF', 'MANH', 'SMAR', 'DOCU', 'BOX',
    'RNG', 'FIVN', 'NICE',

    # === MORE TECHNOLOGY - HARDWARE & SEMIS (20) ===
    'MRVL', 'ON', 'SWKS', 'QRVO', 'MPWR', 'ALGM', 'CRUS', 'SYNA', 'DIOD', 'POWI',
    'SMTC', 'WOLF', 'ACLS', 'MKSI', 'COHR', 'IPGP', 'NOVT', 'TER', 'ENTG', 'AMKR',

    # === MORE ENERGY (15) ===
    'PSX', 'TRGP', 'LNG', 'DINO', 'PBF', 'MTDR', 'PR', 'CTRA', 'AR', 'RRC',
    'SWN', 'EQT', 'CNX', 'NOG', 'CHRD',

    # === MORE MATERIALS (15) ===
    'VMC', 'MLM', 'STLD', 'CLF', 'X', 'CMC', 'ATI', 'RS', 'AA', 'CENX',
    'HUN', 'OLN', 'WLK', 'EMN', 'CE',

    # === MORE COMMUNICATION SERVICES (10) ===
    'LBRDK', 'LBRDA', 'FWONK', 'LYV', 'MTCH', 'IAC', 'PARA', 'FOX', 'FOXA', 'NYT',

    # === INTERNATIONAL ADRs - EUROPE (15) ===
    'SAP', 'ASML', 'AZN', 'GSK', 'NVO', 'DEO', 'RIO', 'BHP', 'VALE', 'SHEL',
    'BP', 'EQNR', 'TTE', 'SAN', 'ING',

    # === INTERNATIONAL ADRs - ASIA & EMERGING (10) ===
    'INFY', 'WIT', 'HDB', 'IBN', 'SONY', 'MUFG', 'SMFG', 'KB', 'SHG', 'LFC',

    # === CONSUMER STAPLES - MORE COVERAGE (10) ===
    'HSY', 'MNST', 'COKE', 'KDP', 'EL', 'KVUE', 'SYY', 'USFD', 'PFGC', 'CHEF',

    # === MISCELLANEOUS VALUE (10) ===
    'BRO', 'ERIE', 'WRB', 'RLI', 'CINF', 'GL', 'PRI', 'AIZ', 'FAF', 'FNF',

    # === AIRLINES & TRAVEL (12) ===
    'CCL', 'RCL', 'DAL', 'UAL', 'LUV', 'AAL', 'JBLU', 'NCLH', 'ABNB', 'EXPE',
    'TRIP', 'TCOM',

    # === EV & CLEAN ENERGY (25) ===
    'NIO', 'XPEV', 'LI', 'NKLA', 'GOEV', 'WKHS', 'HYLN', 'CHPT', 'BLNK', 'EVGO',
    'QS', 'MVST', 'ENVX', 'FREY', 'SLDP', 'SEDG', 'ENPH', 'RUN', 'NOVA', 'ARRY',
    'FSLR', 'JKS', 'STEM', 'BE', 'FCEL',

    # === BITCOIN MINERS & CRYPTO (15) ===
    'MARA', 'CLSK', 'RIOT', 'BITF', 'HUT', 'CIFR', 'WULF', 'CORZ', 'BTDR', 'IBIT',
    'GBTC', 'ETHE', 'BITO', 'BITX', 'MSTU',

    # === AI & DATA CENTERS (15) ===
    'CRWV', 'APLD', 'AI', 'BBAI', 'SOUN', 'SMCI', 'VRT', 'ANET', 'AKAM', 'FSLY',
    'NEWR', 'LUMN', 'NBIS', 'APP',

    # === SPACE & DEFENSE (11) ===
    'SPCE', 'PL', 'BKSY', 'IRDM', 'GSAT', 'KTOS', 'RCAT', 'JOBY', 'ACHR', 'BLDE',
    'MNTS',

    # === QUANTUM COMPUTING (2) ===
    'QUBT', 'QBTS',

    # === CANNABIS (8) ===
    'TLRY', 'SNDL', 'CGC', 'ACB', 'CRON', 'GRWG', 'CURLF', 'GTBIF',

    # === BIOTECH RETAIL FAVORITES (20) ===
    'NVAX', 'OCGN', 'BCRX', 'VXRT', 'MNKD', 'GERN', 'FATE', 'XENE', 'CORT', 'PRTA',
    'IMVT', 'VRNA', 'AXSM', 'VNDA', 'TGTX', 'PCRX', 'JAZZ', 'TEVA', 'DNA', 'TEM',

    # === CHINA TECH ADRs (12) ===
    'PDD', 'JD', 'BIDU', 'NTES', 'BILI', 'TME', 'IQ', 'FUTU', 'TIGR', 'GRAB',
    'SE', 'CPNG',

    # === FINTECH (10) ===
    'LC', 'MELI', 'LMND', 'ROOT', 'OPEN', 'RDFN', 'Z', 'CVNA', 'KMX',

    # === GAMING & STREAMING (15) ===
    'RBLX', 'U', 'TTWO', 'EA', 'DKNG', 'RSI', 'GENI', 'ROKU', 'SPOT', 'TTD',
    'MGNI', 'PUBM', 'DV', 'IAS', 'RDDT',

    # === CONSUMER/RETAIL (18) ===
    'CHWY', 'W', 'ETSY', 'EBAY', 'RVLV', 'GPS', 'ANF', 'AEO', 'URBN', 'M',
    'JWN', 'KSS', 'DDS', 'BIG', 'PTON', 'GPRO', 'SNAP', 'PINS',

    # === MEME & SPECULATIVE (10) ===
    'CLOV', 'SIRI', 'NOK', 'PLUG', 'WISH', 'CPRX', 'RITM', 'BMBL', 'DUOL', 'DJT',

    # === CYBERSECURITY (6) ===
    'CYBR', 'TENB', 'RPD', 'VRNS', 'QLYS', 'SAIC',

    # === HOTELS & CASINOS (7) ===
    'LVS', 'MGM', 'H', 'IHG', 'CHH', 'WH', 'PLYA',

    # === MISC TECH (5) ===
    'DLB', 'APPS', 'DOCN', 'MQ',
]

# Remove duplicates while preserving order
STOCK_UNIVERSE = list(dict.fromkeys(STOCK_UNIVERSE))


def fetch_weekly_data(symbol: str) -> Optional[pd.DataFrame]:
    """
    Fetch weekly price data from Yahoo Finance.
    
    Returns DataFrame with columns: Open, High, Low, Close, Volume
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", interval="1wk")
        
        if df.empty:
            print(f"  ✗ No data returned for {symbol}")
            return None
        
        # Rename columns for consistency
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # Use Close as adjusted_close (yfinance already adjusts by default)
        df['adjusted_close'] = df['close']
        
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
    df = df.copy()
    df['below_line'] = df['adjusted_close'] < df['WMA_200']
    df['cross_below'] = (df['below_line']) & (~df['below_line'].shift(1).fillna(False).astype(bool))
    
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
        # Still process stocks with less data, just won't have full 200WMA history
        if len(df) < 50:
            return None
    
    # === 200-WEEK MOVING AVERAGE ===
    df['WMA_200'] = df['adjusted_close'].rolling(window=200, min_periods=50).mean()
    
    # === DISTANCE FROM 200WMA ===
    df['pct_from_wma'] = ((df['adjusted_close'] - df['WMA_200']) / df['WMA_200']) * 100
    
    # === WEEK-OVER-WEEK DIRECTIONAL CHANGE ===
    df['wow_change'] = df['pct_from_wma'] - df['pct_from_wma'].shift(1)
    
    # === 14-WEEK RSI ===
    df['RSI_14'] = calculate_rsi(df['adjusted_close'], periods=14)
    
    # === HISTORICAL TOUCHES ===
    df_complete = df.dropna(subset=['WMA_200'])
    if len(df_complete) == 0:
        print(f"  ✗ {symbol}: No valid WMA data")
        return None
    
    historical_touches = find_historical_touches(df_complete.copy())
    
    # Get latest values
    latest = df_complete.iloc[-1]
    
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
        'wow_change': round(float(latest['wow_change']), 2) if pd.notna(latest['wow_change']) else 0.0,
        'rsi_14': round(float(latest['RSI_14']), 1) if pd.notna(latest['RSI_14']) else 50.0,
        'below_line': bool(latest['adjusted_close'] < latest['WMA_200']),
        'approaching': float(latest['wow_change']) < 0 if pd.notna(latest['wow_change']) else False,
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
        'approaching_stocks': approaching_sorted[:10],
        'oversold_stocks': [s for s in stocks if s['rsi_14'] < 30],
    }


def main():
    """Main entry point for weekly data update."""
    print("=" * 60)
    print("Below The Line - Weekly Data Update (Yahoo Finance)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
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
        
        # Small delay to be respectful to Yahoo Finance
        if i % 50 == 0 and i > 0:
            print(f"  ... processed {i}/{len(STOCK_UNIVERSE)} stocks")
            time.sleep(1)
    
    print("-" * 60)
    print(f"Successfully processed: {len(results)}/{len(STOCK_UNIVERSE)}")
    if errors:
        print(f"Errors ({len(errors)}): {', '.join(errors[:20])}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")
    
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
