import os
import re
import time
import datetime
from pathlib import Path

from .ssh import ssh_connection
from .utils import clean_sql, alert
from .config import BEELINE_CONFIG

def beeline_session(shell, queue_name=None, timeout=10):
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

def extract_query_output(output):
    output = output.replace('\r\n', '\n')
    lines = output.splitlines()

    # Locate the start of the result table (first "+---" line)
    table_start_idx = next((i for i, line in enumerate(lines) if line.strip().startswith("+") and "-" in line), None)
    if table_start_idx is None:
        return output.strip(), ""

    # Locate the last "+---" line which ends the table
    table_end_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip().startswith("+") and "-" in lines[i]:
            table_end_idx = i
            break

    # Find the row summary (e.g., "10 rows selected") after table end
    rows_line = ""
    if table_end_idx is not None:
        for i in range(table_end_idx + 1, len(lines)):
            if re.match(r"^\d+\srows selected|No rows selected|1 row selected", lines[i].strip()):
                rows_line = lines[i].strip()
                break

    table_output = "\n".join(lines[table_start_idx:table_end_idx + 1])
    return table_output.strip(), rows_line


def run_sql(sql_query, queue_name=None, io=True, timeout=0, log_enabled=True):
    if queue_name is None:
        queue_name = BEELINE_CONFIG().get("DEFAULT_QUEUE", "")

    ssh_client, shell = ssh_connection()
    beeline_session(shell, queue_name)

    sql_query = clean_sql(sql_query)
    while shell.recv_ready():
        shell.recv(65535)
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
            if any(x in new_data for x in ["rows selected", "No rows selected", "row selected", "Error"]):
                alert()
                break
        else:
            time.sleep(0.001)

    query_output, rows = extract_query_output(output)

    log_file_path = None
    if log_enabled:
        logs_dir = os.path.normpath(os.path.expanduser("~/pb_logs"))
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
            log_file.write(query_output + "\n" + rows + "\n")

    if "Error" in output and log_file_path:
        file_name_only = os.path.basename(log_file_path)
        error_message_html = f"""
        <p>An error occurred, click to see the logs: 
        <a href='{log_file_path}' target='_blank'>{file_name_only}</a></p>
        """

    if io:
        print(query_output)
        if rows:
            print(rows)

    ssh_client.close()
    time.sleep(0.5)

    return query_output, rows
