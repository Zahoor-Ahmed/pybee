"""
pybee: A modular Python library for executing SQL queries over Beeline, managing remote file operations,
and handling data transformation tasks in big data environments.

This package provides high-level utilities for:
- Establishing SSH connections and running shell commands.
- Running SQL queries using a Beeline session over SSH.
- Uploading/downloading files and tables between local and remote systems.
- Data export, formatting, and time-based partition utilities.
- Integration with Jupyter via SQL magic commands.
"""

# Import public API from submodules

from .ssh import ssh_connection, run_shell
from .core import beeline_session, run_sql
from .fileops import upload_file, download_file, df_to_Table, table_to_df, download_df
from .utils import alert, text_to_df, to_sql_inlist, todayx, this_monthx, export, daypartitions, daypartitions_to_sec, set_env
# from .meta import confirm_table_size
try:
    from .ipython import register_sql_magic
    register_sql_magic()
except:
    pass  # Ignore IPython magic registration in non-IPython environments

__all__ = [
    'ssh_connection', 'run_shell',
    'beeline_session', 'run_sql',
    'upload_file', 'download_file', 'df_to_Table', 'table_to_df', 'download_df',
    'alert', 'text_to_df', 'to_sql_inlist', 'todayx', 'this_monthx', 'export', 'daypartitions', 'daypartitions_to_sec', 'set_env',
    'register_sql_magic', 'confirm_table_size'
]

# Inject current date values as global constants for convenience
from datetime import datetime
import builtins

# Define `today` and `this_month` as global constants (days/months since 1970)
builtins.today = (datetime.now() - datetime(1970, 1, 1)).days
builtins.this_month = (datetime.now().year - 1970) * 12 + datetime.now().month - 1
