from decimal import Decimal


def cagr(initial, final, years=None, months=None, days=None):
    '''
    Calculate the Compound Annual Growth Rate of an investment,
    given its value at the start and end of a period, as well as
    the length of the period in years, months, or days.

    The result is returned as a Decimal.
    '''
    initial = Decimal(initial)
    if initial == Decimal(0):
        raise Exception('The initial value cannot be zero')

    final = Decimal(final)

    if years is not None:
        years = Decimal(years)
    elif months is not None:
        years = Decimal(months) / Decimal(12.0)
    elif days is not None:
        years = Decimal(days) / Decimal(365.0)
    else:
        raise Exception('Missing years, months, or days value')

    if years == Decimal(0):
        return None

    return (final / initial) ** (Decimal(1) / years) - Decimal(1)
