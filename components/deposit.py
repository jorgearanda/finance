from datetime import date
import pandas as pd

from db import db


class Deposit():
    """Basic deposit data.

    Public methods:
    --none yet--

    Instance variables:
    day -- Date in which the deposit took place
    amount -- Amount of the deposit
    """

    def __init__(self, day, amount):
        """Instantiate a Deposit object."""
        self.day = day
        self.amount = amount

    def __repr__(self):
        return f'{self.day}: {self.amount:10.2f}'

    def __str__(self):
        return f'{self.day}: {self.amount:10.2f}'
