from datetime import datetime as dt, date
from freezegun import freeze_time
from pytest import approx

from components.deposit import Deposit
from db import db


def test_deposit_class_instantiates():
    d = Deposit(date(2017, 3, 9), 3000)
    assert d
    assert d.day == date(2017, 3, 9)
    assert d.amount == 3000
    assert d.__repr__() == '2017-03-09:    3000.00'
    assert str(d) == '2017-03-09:    3000.00'
