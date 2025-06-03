"""
core.py

This module provides the core functionality for executing SQL queries over Beeline
via SSH. It includes session management, output formatting, and result handling.
"""

import os
import re
import time
import datetime
# from IPython.display import display, HTML
from pathlib import Path

from .ssh import ssh_connection
from .utils import clean_sql, alert
from .config import BEELINE_CONFIG

def beeline_session(shell, queue_name=None, timeout=10):
    """
    Initiate a Beeline session through a remote shell.

    Args:
        shell: An active shell object obtained from an SSH connection.
        queue_name (str): Name of the resource queue to use in Beeline.
        timeout (int): Maximum wait time in seconds for the connection to establish.

    Raises:
        TimeoutError: If the Beeline session does not connect within the specified timeout.
    """
    config = BEELINE_CONFIG()
    env_path = config.get("env_path", "")
    keytab_path = config.get("keytab_path", "")
    user = config.get("user", "")
    beeline_path = config.get("beeline_path", "")

    if queue_name:
        beeline_command = f"source {env_path}; kinit -kt {keytab_path} {user}; {beeline_path} {queue_name};"
    else:
        beeline_command = f"source {env_path}; kinit -kt {keytab_path} {user}; {beeline_path};"

    shell.send(beeline_command + "\n")
    output = ''
    start_time = time.time()
    while True:
        if shell.recv_ready():
            new_data = shell.recv(65535).decode('utf-8')
            output += new_data
            if "Connecting to jdbc:fiber" in new_data:
                break
        if time.time() - start_time > timeout:
            break
        time.sleep(0.1)



def run_sql(sql_query, queue_name=None, io=True, timeout=0, log_enabled=True):
    """
    Execute an SQL query through Beeline using an SSH shell.

    Args:
        sql_query (str): The SQL query to execute it can contructed dynamically using python formatted strings before passing here.
        queue_name (str): The Beeline resource queue to connect to if configured.
        io (bool): If True, print the output.
        timeout (int): Maximum execution time in seconds.
        log_enabled (bool): If True, enable logging of the query output/errors.

    Returns:
        tuple: A tuple containing the output (str) and row count (str).
    """

    if queue_name is None:
        queue_name = BEELINE_CONFIG().get("DEFAULT_QUEUE","")

    ssh_client, shell = ssh_connection()
    beeline_session(shell,queue_name)

    sql_query = clean_sql(sql_query)
    while shell.recv_ready():
        shell.recv(65535)  # Clear buffer
    shell.send(sql_query + "\n;\n")
    output = ''
    start_time = time.time()
    while True:
        if timeout > 0 and time.time() - start_time > timeout:
            print("Timeout reached, exiting loop.")
            alert()
            break
        if shell.recv_ready():
            new_data = shell.recv(65535).decode('utf-8')
            output += new_data
            if any(pattern in new_data for pattern in ["rows selected", "No rows selected", "row selected", "Error"]):
                alert()
                break
        else:
            time.sleep(0.001)
    
    # ---------------------------
    output = output.replace('\r\n', '\n')           # Normalize line endings
    cleaned_sql = clean_sql(sql_query).strip()      # Use the exact SQL query (may have extra newlines, so strip it)
    sql_index = output.find(cleaned_sql)            # Find where SQL query appears in output
    if sql_index != -1 and "Error" not in output:
        query_output = output[sql_index + len(cleaned_sql):].lstrip()
        pattern = r'\n(\d{1,3}(,\d{3})*\srows selected|No rows selected|1 row selected)'
        parts = re.split(pattern, query_output, maxsplit=1)
        query_output = parts[0]
        rows = "\n" + parts[1] if len(parts) > 1 else ""
    else:
        query_output = output[sql_index + len(cleaned_sql):].lstrip() if sql_index != -1 else output
        rows = ""

    log_file_path = None
    if log_enabled:
        root_dir = Path.cwd()
        logs_dir = f"{root_dir}/xlogs"
        current_year = datetime.datetime.now().strftime("%Y")
        current_month = datetime.datetime.now().strftime("%m")
        year_dir = os.path.join(logs_dir, current_year)
        month_dir = os.path.join(year_dir, current_month)
        os.makedirs(month_dir, exist_ok=True)
        today_date = datetime.datetime.now().strftime("%Y_%m_%d")
        log_file_path = os.path.join(month_dir, f"logs_{today_date}.txt")

        with open(log_file_path, "a", encoding="utf-8") as log_file:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write("\n\n" + "-" * 70 + "\n")
            log_file.write(f"{timestamp}\n")
            log_file.write("-" * 70 + "\n")
            log_file.write("\n" + sql_query + "\n\n")
            log_file.write(query_output.lstrip() + rows + "\n")

    if "Error" in output and log_file_path:
        file_name_only = os.path.basename(log_file_path)
        error_message_html = f"""
        <p>An error occurred, click to see the logs: 
        <a href='{log_file_path}' target='_blank'>{file_name_only}</a></p>
        """
        # display(HTML(error_message_html))

    if io:
        print(f"{query_output}{rows}")

    ssh_client.close()
    time.sleep(0.5)

    return query_output, rows
