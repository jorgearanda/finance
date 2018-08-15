from db import db
from snapshot import snapshot


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_snapshot_does_not_crash(simple):
    snapshot({
        '--accounts': None,
        '--update': False,
        '--verbose': False,
        '--positions': True
    })

    assert True
