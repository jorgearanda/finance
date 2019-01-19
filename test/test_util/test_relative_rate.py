import pandas as pd
from pytest import approx

from util.relative_rate import relative_rate


def test_relative_rate():
    fixed_rate = pd.Series([0.0, 0.5, 1.0, 1.5])
    rel = relative_rate(fixed_rate)

    assert rel[0] == approx(0.0)
    assert rel[1] == approx(0.5)
    assert rel[2] == approx(1 / 3)
    assert rel[3] == approx(1 / 4)
