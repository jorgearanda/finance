from portfolio import Portfolio


p = Portfolio(verbose=False)
latest = p.latest()

print('Portfolio Snapshot')
print('===========================')
print(f'Total Value:  {latest["total_value"]:12,.2f}')
print(f'Profit:       {latest["profit"]:12,.2f}')
print()
print(f'TWRR Annual:  {latest["twrr_annualized"] * 100:12,.2f}%')
print(f'MWRR Annual:  {latest["mwrr_annualized"] * 100:12,.2f}%')
print(f'Total Returns:{latest["returns"] * 100:12,.2f}%')
print()
print(f'Day Profit:   {latest["day_profit"]:12,.2f}')
print(f'Day Returns:  {latest["day_returns"] * 100:12,.2f}%')
