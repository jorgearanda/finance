def relative_rate(series):
    """
    Return the relative rate of change in a Series representing a rate of change
    from a fixed origin.

    The return value is itself a Pandas Series.
    For instance:
      - for a Series with initial values                   20.0, 30.0, 40.0, 50.0
      - the rate of change from the starting point, RS, is  0.0,  0.5,  1.0,  1.5
      - then the relative_rate(RS) is                       0.0,  0.5,  0.3,  0.25
    """
    return (1 + series) / (1 + series.shift(1).fillna(0)) - 1
