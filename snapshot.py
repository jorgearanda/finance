from portfolio import Portfolio


p = Portfolio(verbose=False)
latest = p.latest()
last_month = p.latest_month()
month_profit = latest['profit'] - last_month['profit']
month_returns = month_profit / last_month['total_value']

print('Portfolio Snapshot')
print('===========================================================')
print(f'Total Value:  {latest["total_value"]:12,.2f}')
print(f'Profit:       {latest["profit"]:12,.2f}')
print()
print(
    f'TWRR:         {latest["twrr"] * 100:12,.2f}%     ' +
    f'TWRR Annual:  {latest["twrr_annualized"] * 100:12,.2f}%')
print(
    f'MWRR:         {latest["mwrr"] * 100:12,.2f}%     ' +
    f'MWRR Annual:  {latest["mwrr_annualized"] * 100:12,.2f}%')
print(f'Total Returns:{latest["returns"] * 100:12,.2f}%')
print()
print(f'Day Profit:   {latest["day_profit"]:12,.2f}')
print(f'Day Returns:  {latest["day_returns"] * 100:12,.2f}%')
print()
print(
    f'Month Profit: {month_profit:12,.2f}      ' +
    f'Last Month:   {last_month["month_profit"]:12,.2f}')
print(
    f'Month Returns:{month_returns * 100:12,.2f}%     ' +
    f'Last Month:   {last_month["month_returns"] * 100:12,.2f}%')
