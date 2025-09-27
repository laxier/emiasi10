"""Manual migration helper to add new columns to service_shift_tasks.
Run:
  PYTHONPATH=emiasik-master python emiasik-master/migrate_columns.py
"""
from database import _auto_migrate

if __name__ == '__main__':
    print('Running auto migration...')
    _auto_migrate()
    print('Done.')
