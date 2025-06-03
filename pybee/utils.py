"""
utils.py

This module provides utility functions for logging, output formatting, and time-related
operations useful in querying and exporting data in Big Data environments.
"""
import time
import datetime
import pandas as pd # type: ignore
import re
# import glob
from importlib.resources import files
from pathlib import Path

import os
os.environ['PYGAME_HIDE_SUPPORT_PROMPT'] = "hide"
import pygame # type: ignore
pygame.mixer.init()
time.sleep(0.2)
def alert(index=0):
    """
    Play a system beep sound as a simple alert.
    Useful for notifying users after long-running operations.
    """
    try:
        base = Path(__file__).parent/ "sounds"
        sound_files = sorted(base.glob("*.wav"))
        sound = pygame.mixer.Sound(str(sound_files[index]))
        sound.play()
    except Exception as e:
        print("❌ Failed to play sound:", e)


def to_sql_inlist(series):
    """Convert a Pandas Series to a SQL-compatible IN clause string."""
    return ','.join("'" + series.dropna().astype(str) + "'")

def todayx(r=7, ref=None):
    """
    Print a list of epoch days leading up to the reference date.

    Args:
        r (int): Number of days to look back.
        ref (int or None): Epoch day to use as reference. If None, today is used.
    """
    from datetime import datetime, timedelta
    if ref is None:
        ref = datetime.now().date()
    else:
        # Assuming ref is given as an epoch day, convert it to a date
        ref = (datetime(1970, 1, 1) + timedelta(days=ref)).date()

    start_date = ref - timedelta(days=r)
    for i in range(r + 1):
        sys_date = start_date + timedelta(days=i)
        unix_date = (sys_date - datetime(1970, 1, 1).date()).days

        if sys_date == ref:
            print("-------------------------")
            print(f"{sys_date}   {unix_date}")
            print("-------------------------")
        else:
            print(f"{sys_date}   {unix_date}   {i - r}")

def this_monthx(r=7, ref=None):
    """
    Print a list of epoch months leading up to the reference month.

    Args:
        r (int): Number of months to look back.
        ref (int or None): Epoch month to use as reference. If None, current month is used.
    """
    if ref is None:
        ref = datetime.datetime.now()
    else:
        # Assuming ref is given as a month code, convert it to a date
        years = ref // 12 + 1970
        months = ref % 12 + 1
        ref = datetime.datetime(years, months, 1)

    # Iterate through the months, starting from `r` months before the reference month
    for i in range(r, -1, -1):
        # Calculate the month by subtracting months from the reference date
        month_adjusted = datetime.datetime(ref.year, ref.month, 1) - datetime.timedelta(days=30 * i)
        month_code = (month_adjusted.year - 1970) * 12 + month_adjusted.month - 1
        month_name = f"{month_adjusted.strftime('%b')}-{month_adjusted.year}"

        if i == 0:
            print("-------------------------")
            print(f"{month_name}   {month_code}")
            print("-------------------------")
        else:
            print(f"{month_name}   {month_code}   {-i}")


from datetime import datetime, timedelta
from calendar import monthrange

def daypartitions(epoch_month):
    """
    Given an epoch month (number of months since Jan 1970),
    returns a list of epoch day values for each day in that month.
    """
    # Convert epoch_month to year and month
    years_since_1970, month_offset = divmod(epoch_month, 12)
    year = 1970 + years_since_1970
    month = month_offset + 1  # calendar module uses 1-based months

    # Determine the number of days in the month
    _, num_days = monthrange(year, month)
    
    # Generate list of epoch days for the month
    start_date = datetime(year, month, 1)
    epoch_base = datetime(1970, 1, 1)
    return [(start_date + timedelta(days=i) - epoch_base).days for i in range(num_days)]


def daypartitions_to_sec(day_list):
    return [int(time.mktime(time.strptime(day, "%Y%m%d"))) for day in day_list]


def text_to_df(output):
    """
    Convert raw text output from Beeline SQL execution into a Pandas DataFrame.

    Args:
        output (str): Raw output string from Beeline query execution.

    Returns:
        pd.DataFrame: DataFrame representation of the query result.
    """
    lines = output.strip().splitlines()
    divider_regex = re.compile(r'^[\+\-]+$', re.UNICODE)
    processed_lines = []
    header = None
    counter = 1

    for line in lines:
        if divider_regex.match(line.strip()) and counter == 1:
            counter += 1
            continue
        if counter == 2:
            counter += 1
            header = [col.strip() for col in line.strip('| ').split(' | ')]
            continue
        if divider_regex.match(line.strip()) and counter == 3:
            counter += 1
            continue
        if divider_regex.match(line.strip()) and counter == 4:
            counter = 2
            continue
        else:
            row = [cell.strip() for cell in line.strip('|').split(' | ')]
            if len(row) < len(header):
                row += [None] * (len(header) - len(row))
            if len(row) == len(header):
                processed_lines.append(row)
    df = pd.DataFrame(processed_lines, columns=header)
    return df


def clean_sql(sql):
    lines = sql.strip().splitlines()
    cleaned_lines = []
    for line in lines:
        stripped = line.lstrip()
        if not stripped.startswith("--"):
            cleaned_lines.append(stripped.replace("\t", " "))
    return " ".join(cleaned_lines).replace(";", "") + ";"


from tkinter.filedialog import asksaveasfilename
from tkinter import Tk

def export(output):
    df = text_to_df(output)

    # Hide the root tkinter window
    root = Tk()
    root.withdraw()
    root.lift()
    root.attributes("-topmost", True)
    root.after(0, root.focus_force)

    # Prompt the user to select a file save location
    file_path = asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        title="Save DataFrame as CSV"
    )

    # If the user selects a file, save the DataFrame to the specified path
    if file_path:
        df.to_csv(file_path, index=False)
        print(f"DataFrame saved to {file_path}")
    else:
        print("Save operation cancelled.")


def set_env(interactive=True):
    """
    Set up the pybee configuration. Creates ~/.pybee_config.json either interactively
    or with placeholder values. Prompts before overwriting if config already exists.
    SSH_CONFIG and WINSCP_CONFIG share server, username, and password fields.

    Args:
        interactive (bool): If True, prompts user for config. If False, uses placeholders.
    """
    import os, json, webbrowser, platform, subprocess

    path = os.path.normpath(os.path.expanduser("~/.pybee_config.json"))

    # Confirm overwrite if file exists
    if os.path.exists(path):
        resp = input(f"\n⚠️ Config file already exists at:\n{path}\nDo you want to overwrite it? (y/N): ").strip().lower()
        if resp != "y":
            print("❌ Aborting. Configuration not changed.")
            return

    if interactive:
        print("\n🔧 Interactive setup mode. Please fill in all configuration fields.\n")

        def ask(prompt):
            val = ""
            while not val:
                val = input(f"{prompt}: ").strip()
            return val

        # Shared SSH values
        ssh_server_ip = ask("SSH Server IP")
        ssh_port = int(ask("SSH Port"))
        ssh_username = ask("SSH Username")
        ssh_password = ask("SSH Password")
        ssh_root_password = ask("Root Password")

        config = {
            "SSH_CONFIG": {
                "server_ip": ssh_server_ip,
                "port": ssh_port,
                "username": ssh_username,
                "password": ssh_password,
                "root_password": ssh_root_password
            },
            "BEELINE_CONFIG": {
                "env_path": ask("Environment script path"),
                "keytab_path": ask("Kerberos keytab path"),
                "user": ask("Kerberos user"),
                "beeline_path": ask("Beeline path"),
                "DEFAULT_QUEUE": ask("Default queue name")
            },
            "WINSCP_CONFIG": {
                "winscp_path": ask("WinSCP executable path"),
                "server_address": ssh_server_ip,  # Reuse SSH values
                "username": ssh_username,
                "password": ssh_password,
                "remote_path": ask("Remote path for uploads"),
                "hostkey": ask("SSH host key fingerprint"),
                "export_dir": ask("Export directory path")
            }
        }

    else:
        print("\n🛠️ Creating config with placeholder values...")
        config = {
            "SSH_CONFIG": {
                "server_ip": "e.g., 10.0.0.1",
                "port": 22,
                "username": "your_ssh_username",
                "password": "your_ssh_password",
                "root_password": "your_root_password"
            },
            "BEELINE_CONFIG": {
                "env_path": "/path/to/env_script",
                "keytab_path": "/path/to/your.keytab",
                "user": "kerberos_user",
                "beeline_path": "/path/to/beeline",
                "DEFAULT_QUEUE": "your_default_queue"
            },
            "WINSCP_CONFIG": {
                "winscp_path": "C:\\Path\\To\\WinSCP.exe",
                "server_address": "same_as_ssh_server_ip",
                "username": "same_as_ssh_username",
                "password": "same_as_ssh_password",
                "remote_path": "/remote/upload/path",
                "hostkey": "host key fingerprint here",
                "export_dir": "/export/path"
            }
        }

    # Save the config file
    with open(path, "w") as f:
        json.dump(config, f, indent=4)

    print(f"✅ Configuration saved to:\n{path}")
    print("You can manually edit this file later to update any values.")

    # Attempt to open the config file
    try:
        if platform.system() == "Windows":
            os.startfile(path)
        elif platform.system() == "Darwin":
            subprocess.call(["open", path])
        else:
            subprocess.call(["xdg-open", path])
    except Exception as e:
        print(f"\n⚠️ Could not open config file automatically: {e}")
        print(f"Please open it manually at: {path}")




