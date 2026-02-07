#!/usr/bin/env python3
"""Quick test of return_to_now for AAPL touches."""
import sys
sys.path.insert(0, 'scripts')
from update_stocks import fetch_spy_monthly, calculate_stock_signals

spy_monthly = fetch_spy_monthly()
result = calculate_stock_signals('AAPL', spy_monthly=spy_monthly)

print(f"Current price: ${result['close']}")
print(f"Touch count: {result['touch_count']}")
print()
print(f"{'Date':<12} {'1yr Return':>12} {'Return Since':>14}")
print("-" * 40)
for t in result['historical_touches']:
    r1 = f"{t['return_1yr']:+.1f}%" if t.get('return_1yr') is not None else 'N/A'
    rn = f"{t['return_to_now']:+.1f}%" if t.get('return_to_now') is not None else 'N/A'
    print(f"{t['date']:<12} {r1:>12} {rn:>14}")
