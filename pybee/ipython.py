# pybee/ipython.py

from IPython.core.magic import register_cell_magic
from .core import run_sql

def register_sql_magic():
    try:
        from IPython import get_ipython
        ip = get_ipython()
        if ip is None:
            return  # Not in an IPython environment

        @register_cell_magic
        def sql(line, cell):
            run_sql(cell, io=True)

        ip.register_magic_function(sql, 'cell')
    except Exception as e:
        print(f"⚠️ Failed to register SQL magic: {e}")
