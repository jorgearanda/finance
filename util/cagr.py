from decimal import Decimal


def cagr(initial, final, years=None, months=None, days=None):
    """
    Calculate the Compound Annual Growth Rate of an investment

    Parameters
    ----------
    initial: float
        Value of the investment at the start of the period
    final: float
        Value of the investment at the end of the period
    years: float, optional
        Length of the period, in years. Takes precedence over `months` and `days`
    months: float, optional
        Length of the period, in months. Takes precedence over `days`
    days: float, optional
        Length of the period, in days. One of the duration fields (`years`, `months`, `days`) must be present

    Returns
    -------
    Compound Annual Growth Rate with a Decimal type
    """
    initial = Decimal(initial)
    if initial == Decimal(0):
        raise Exception("The initial value cannot be zero")

    final = Decimal(final)

    if years is not None:
        years = Decimal(years)
    elif months is not None:
        years = Decimal(months) / 12
    elif days is not None:
        years = Decimal(days) / 365
    else:
        raise Exception("Missing years, months, or days value")

    if years == Decimal(0):
        return None

    return (final / initial) ** (1 / years) - 1
