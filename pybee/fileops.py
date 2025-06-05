# pybee/fileops.py

import os
import re
import pandas as pd # type: ignore
import time
import tempfile
import subprocess
from pathlib import Path
import tkinter as tk
from tkinter import Tk, filedialog, messagebox

from .utils import alert, text_to_df
from .config import WINSCP_CONFIG, BEELINE_CONFIG
from .core import run_sql
from .ssh import run_shell


def check_winscp_installed():
    """
    Validates that WinSCP is installed by checking the configured path.
    Raises a FileNotFoundError if the executable is missing.
    """
    winscp_path = WINSCP_CONFIG().get("winscp_path", "")
    if not winscp_path or not Path(winscp_path).is_file():
        raise FileNotFoundError(f"❌ WinSCP not found at: {winscp_path}. Please check your WINSCP_CONFIG.")

def upload_file(remote_path=None):
    """
    Uploads a CSV file to a remote server using WinSCP, sanitizes filename, and reports outcome.
    """
    check_winscp_installed()
    # Step 1: File selection via dialog
    root = Tk()
    root.withdraw()
    root.lift()
    root.attributes("-topmost", True)
    root.after(0, root.focus_force)

    csv_file_path = filedialog.askopenfilename(title="Select a File")
    if not csv_file_path:
        print("❌ No file selected.")
        return

    csv_path = Path(csv_file_path)
    directory_path = csv_path.parent
    new_file_name = csv_path.name.replace(" ", "_")
    new_file_path = directory_path / new_file_name

    if csv_path.name != new_file_name:
        os.rename(csv_path, new_file_path)

    csv_file_path = str(new_file_path)
    remote_file_path = f"{remote_path}/{new_file_name}" if remote_path else new_file_name

    project_root = Path(__file__).resolve().parent.parent
    log_dir = os.path.normpath(os.path.expanduser("~/pb_logs/winscp"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(log_dir / "WinSCP.log")

    # Load config safely
    config = WINSCP_CONFIG()
    winscp_path = config.get("winscp_path", "")
    username = config.get("username", "")
    password = config.get("password", "")
    server_address = config.get("server_address", "")
    hostkey = config.get("hostkey", "")

    print("1. Uploading file to the server", end="")

    winscp_command = (
        f'"{winscp_path}" /console /log="{log_path}" /loglevel=* /ini=nul '
        f'/command "open sftp://{username}:{password}@{server_address}/ -hostkey=""{hostkey}""" '
        f'"put \"{csv_file_path}\" \"{remote_file_path}\"" "exit"'
    )

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bat", mode="w", dir=str(directory_path)) as script_file:
            script_file.write(winscp_command)
            script_path = script_file.name

        result = subprocess.run(script_path, text=True)

        if result.returncode == 0:
            print(" . . . . . . . . . . . . . . . . . . . . . . . . ~ Done.")
        else:
            print(f"\n❌ Error during upload. Return code: {result.returncode}")
            print("📄 Check the WinSCP log for details:", log_path)

    except FileNotFoundError:
        print("❌ WinSCP not found. Check WINSCP_CONFIG().get('winscp_path')")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        if os.path.exists(script_path):
            os.remove(script_path)
    alert()

#--------------------------------------------------------------------------------------------------------------------------------

def download_file(remote_file_path):
    """
    Downloads a file from the specified path on the server to the local machine using WinSCP.
    In the argument "remote_file_path" provide full file path.
    """
    check_winscp_installed()
    
    # Prompt for save location
    root = Tk()
    root.withdraw()
    root.lift()
    root.attributes("-topmost", True)
    root.after(0, root.focus_force)

    local_save_path = filedialog.asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv")],
        title="Save exported CSV file"
    )

    if not local_save_path:
        print("❌ Save operation cancelled.")
        return

    # Normalize Windows path (fix slashes)
    local_save_path = os.path.normpath(local_save_path)

    # Prepare log directory
    project_root = Path(__file__).resolve().parent.parent
    log_dir = os.path.normpath(os.path.expanduser("~/pb_logs/winscp"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(log_dir / "WinSCP.log")

    # Load config safely
    config = WINSCP_CONFIG()
    winscp_path = config.get("winscp_path", "")
    username = config.get("username", "")
    password = config.get("password", "")
    server_address = config.get("server_address", "")
    hostkey = config.get("hostkey", "")

    print("8. Downloading CSV file to local machine", end="")

    # Build command with proper quoting
    winscp_command = (
        f'"{winscp_path}" /console /log="{log_path}" /loglevel=* /ini=nul '
        f'/command "open sftp://{username}:{password}@{server_address}/ -hostkey=""{hostkey}""" '
        f'"get \"{remote_file_path}\" \"{local_save_path}\"" "exit"'
    )

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bat", mode="w") as script_file:
            script_file.write(winscp_command)
            script_path = script_file.name

        result = subprocess.run(script_path, text=True)

        if result.returncode == 0:
            print(" . . . . . . . . . . . . . . . . . . . . . . . . ~ Done.")
            print("File saved to ",local_save_path)
            return pd.read_csv(local_save_path)
        else:
            print(f"\n❌ Error during download. Return code: {result.returncode}")
            print("📄 Check the WinSCP log at:", log_path)
            return None
    except FileNotFoundError:
        print("❌ WinSCP not found. Check WINSCP_CONFIG['winscp_path']")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None
    finally:
        if os.path.exists(script_path):
            os.remove(script_path)

    alert()

#--------------------------------------------------------------------------------------------------------------------------------

def download_df(remote_file_path,
                local_dir=os.path.expanduser("~/Downloads/db_exports")):

    os.makedirs(local_dir, exist_ok=True)

    file_name = os.path.basename(remote_file_path)
    local_file_path = os.path.join(local_dir, file_name)

    # Prompt if file exists
    if os.path.exists(local_file_path):
        root = Tk()
        root.withdraw()
        root.lift()
        root.attributes("-topmost", True)
        root.after(0, root.focus_force)

        response = messagebox.askyesno(
            "File Exists",
            f"'{file_name}' already exists in:\n{local_dir}\n\nDo you want to overwrite it?"
        )
        if not response:
            load_existing = messagebox.askyesno(
                "Load Existing",
                "Do you want to load DataFrame from the existing file instead?"
            )
            if load_existing:
                print(f"\nLoaded existing file from:\n\"{os.path.normpath(os.path.dirname(local_file_path))}\"")
                return pd.read_csv(local_file_path)
            else:
                print("\nOperation cancelled. No DataFrame returned.")
                return None

    # Load config safely
    config = WINSCP_CONFIG()
    winscp_path = config.get("winscp_path", "")
    username = config.get("username", "")
    password = config.get("password", "")
    server_address = config.get("server_address", "")
    hostkey = config.get("hostkey", "")

    project_root = Path(__file__).resolve().parent.parent
    log_dir = os.path.normpath(os.path.expanduser("~/pb_logs/winscp"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(log_dir / "WinSCP.log")
    
    # Build WinSCP command
    winscp_command = (
        f'"{winscp_path}" /console /log="{log_path}" /loglevel=* /ini=nul '
        f'/command "open sftp://{username}:{password}@{server_address}/ -hostkey=""{hostkey}""" '
        f'"get ""{remote_file_path}"" ""{local_file_path}""" "exit"'
    )

    try:
        bat_path = os.path.join(local_dir, "winscp_download.bat")
        with open(bat_path, "w") as script_file:
            script_file.write(winscp_command)

        result = subprocess.run(bat_path, text=True)

        if result.returncode == 0:
            tablename = file_name.rsplit(".csv", 1)[0]
            print(f"\nTable {tablename} downloaded successfully in:\n\"{os.path.normpath(os.path.dirname(local_file_path))}\"")
            return pd.read_csv(local_file_path)
        else:
            print(f"Download failed. Check the log at:\n{log_path}")
            return None
    finally:
        if os.path.exists(bat_path):
            os.remove(bat_path)


#--------------------------------------------------------------------------------------------------------------------------------


def confirm_table_size(table_name):
    query = f"DESCRIBE FORMATTED {table_name}"
    output, _ = run_sql(query, io=0)
    df = text_to_df(output)

    size_bytes = 0

    try:
        # Case 1: Try "Statistics" field
        stat_row = df[df['col_name'].str.strip() == 'Statistics']
        if not stat_row.empty:
            size_text = stat_row['data_type'].values[0]
            size_bytes = int(size_text.split()[0]) if size_text else 0
        else:
            # Case 2: Try "Table Data Size" field like "2.99TB"
            alt_row = df[df['col_name'].str.strip() == 'Table Data Size']
            if not alt_row.empty:
                value = alt_row['data_type'].values[0]
                num, unit = float(value[:-2]), value[-2:].upper()
                multiplier = {
                    'KB': 1_000,
                    'MB': 1_000_000,
                    'GB': 1_000_000_000,
                    'TB': 1_000_000_000_000,
                    'PB': 1_000_000_000_000_000
                }.get(unit, 1)
                size_bytes = int(num * multiplier)
    except Exception:
        size_bytes = 0

    def human_readable_size(size):
        for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if size < 1000:
                return f"{size:.2f} {unit}"
            size /= 1000
        return f"{size:.2f} PB"

    readable_size = human_readable_size(size_bytes)

    root = Tk()
    root.withdraw()
    root.lift()
    root.attributes("-topmost", True)
    root.after(0, root.focus_force)

    proceed = messagebox.askyesno(
        "Table Size Check",
        f"The size of table '{table_name}' is estimated to be:\n\n{readable_size}\n\nDo you want to continue with the export?",
        parent=root
    )
    root.destroy()
    return proceed

def table_to_df(table_name):
    """
    Exports a Hive table to CSV, saves to remote, and downloads locally.
    """
    if not confirm_table_size(table_name):
        print("Export cancelled by user.")
        return None

    try:
        def print_done(message, total_width=100):
            done_text = " ~ Done."
            remaining_space = total_width - len(message) - len(done_text)
            extra_space = ' ' if remaining_space % 2 else ''
            dots = " ." * (remaining_space // 2)
            print(f"{extra_space}{dots}{done_text}")

        # Load paths from config
        export_dir = WINSCP_CONFIG().get("export_dir", "")
        remote_dir = WINSCP_CONFIG().get("remote_path", "")
        hdfs_env = BEELINE_CONFIG().get("env_path", "")
        keytab = BEELINE_CONFIG().get("keytab_path", "")
        beeline_user = BEELINE_CONFIG().get("user", "")

        # 1. Get column headers
        print("1. Fetching column headers", end="")
        schema_query = f"DESCRIBE {table_name}"
        output, _ = run_sql(schema_query, io=0)
        df = text_to_df(output)
        header_row = ','.join(df['col_name'].astype(str).tolist())
        print_done("1. Fetching column headers")

        # 2. Export to HDFS
        print("2. Exporting table to HDFS directory", end="")
        hdfs_output_path = f"{export_dir}/{table_name}"
        query = f"""
        INSERT OVERWRITE DIRECTORY '{hdfs_output_path}' 
        ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY ',' 
        SELECT {', '.join([f"COALESCE({col}, '')" for col in df['col_name']])}
        FROM {table_name}
        """
        run_sql(query, io=0)
        print_done("2. Exporting table to HDFS directory")

        # 3. Combine parts into a single CSV file
        print("3. Combining parts into a single CSV file", end="")
        final_csv_path = f"{remote_dir}/{table_name}.csv"
        tmp_csv_path = f"{remote_dir}/tmp_{table_name}.csv"
        getmerge_command = f"""
        source {hdfs_env};
        kinit -kt {keytab} {beeline_user};
        hdfs dfs -getmerge {hdfs_output_path} {tmp_csv_path};
        echo "{header_row}" > {final_csv_path};
        cat {tmp_csv_path} >> {final_csv_path};
        rm -f {tmp_csv_path}
        """
        run_shell(getmerge_command)
        print_done("3. Combining parts into a single CSV file")

        # 4. Clean up HDFS
        print("4. Removing files from HDFS", end="")
        hdfs_remove_command = f"""
        source {hdfs_env};
        kinit -kt {keytab} {beeline_user};
        hdfs dfs -rm -r {hdfs_output_path};
        """
        run_shell(hdfs_remove_command)
        print_done("4. Removing files from HDFS")

        # 5. Fix file permissions
        print("5. Changing file permissions", end="")
        chmod_command = f"chmod 644 {final_csv_path}"
        run_shell(chmod_command)
        print_done("5. Changing file permissions")

        # 6. Download and return as DataFrame
        df_local = download_df(remote_file_path=final_csv_path)
        return df_local

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None



#--------------------------------------------------------------------------------------------------------------------------------

def df_to_Table(df, df_name="default_df"):
    """
    Uploads a DataFrame to a remote server as a temporary CSV, removes its header, transfers it to HDFS,
    and creates a database table from it.
    """
    try:
        # Step 1: Write the DataFrame to a temporary CSV file
        print("1: Writing DataFrame to a temporary CSV file", end="")
        df = df.astype(str)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w") as temp_csv:
            temp_csv_path = temp_csv.name
            df.to_csv(temp_csv_path, index=False)
        print(" . . . . . . . . . . . . . . . . . . . . ~ Done.")

        # Prepare file paths and variables
        csv_file_path = temp_csv_path
        new_file_name = os.path.basename(csv_file_path)
        directory_path = os.path.dirname(csv_file_path)
        remote_path = WINSCP_CONFIG().get("remote_path", "")
        remote_file_path = f"{remote_path}/{new_file_name}"

        # Step 2: Upload the file to the server
        print("2: Uploading file to the server", end="")
        log_path = os.path.join(directory_path, "WinSCP.log")
        winscp_cfg = WINSCP_CONFIG()
        winscp_command = (
            f'"{winscp_cfg["winscp_path"]}" /console /log="{log_path}" /loglevel=* /ini=nul '
            f'/command "open sftp://{winscp_cfg["username"]}:{winscp_cfg["password"]}@{winscp_cfg["server_address"]}/ -hostkey=""{winscp_cfg["hostkey"]}""" '
            f'"put ""{csv_file_path}"" ""{remote_file_path}""" "exit"'
        )

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bat", mode="w", dir=directory_path) as script_file:
            script_file.write(winscp_command)
            script_path = script_file.name

        result = subprocess.run(script_path, text=True)
        if result.returncode == 0:
            print(" . . . . . . . . . . . . . . . . . . . . . . . . . .  ~ Done.")
        else:
            print(f"\nError during upload. Return code: {result.returncode}")
            return

        # Step 3: Remove headers on the server
        print("3: Removing file headers on the server", end="")
        remove_header_command = f"sed -i '1d' {remote_file_path}"
        run_shell(remove_header_command)
        print(" . . . . . . . . . . . . . . . . . . . . . . . ~ Done.")

        # Step 4: Move file to HDFS
        print("4: Moving file to HDFS", end="")
        beeline_cfg = BEELINE_CONFIG()
        hdfs_command = f"""
        source {beeline_cfg["env_path"]};
        kinit -kt {beeline_cfg["keytab_path"]} {beeline_cfg["user"]};
        hdfs dfs -rm /srv/smartcare/calc_input/cn/tmp*.csv;
        hdfs dfs -put -f "{remote_file_path}" "/srv/smartcare/calc_input/cn/{new_file_name}";
        hdfs dfs -ls -h "/srv/smartcare/calc_input/cn/{new_file_name}";
        """
        run_shell(hdfs_command)
        print(" . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ~ Done.")

        # Step 5: Table name input dialog
        print("5: Table creation started", end="")
        def clean_filename(filename):
            return re.sub(r'\W+', '_', filename.strip()).lower()

        def get_table_name(default_name):
            root = Tk()
            root.geometry("300x100")
            root.title("Table Name")
            root.lift()
            root.attributes('-topmost', True)
            root.focus_force()

            tk.Label(root, text="Enter table name:").pack(pady=5)
            entry = tk.Entry(root, width=30)
            entry.insert(0, default_name)
            entry.pack(pady=5)

            table_name = None

            def on_submit():
                nonlocal table_name
                table_name = entry.get().strip()
                root.quit()
                root.destroy()

            tk.Button(root, text="OK", command=on_submit).pack(pady=5)
            root.mainloop()
            return table_name

        default_table_name = clean_filename(df_name)
        table_name = get_table_name(default_table_name)

        # Step 6: Create the table
        df_sample = df.head(0)
        columns = [re.sub(r'\W+', '_', col.strip()).lower() for col in df_sample.columns]
        column_definitions = [f"{col} STRING" for col in columns]

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_definitions)}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        """
        run_sql(query, io=False)
        print(" . . . . . . . . . . . . . . . . . . . . . . . . . . . . .  ~ Done.")

        # Step 7: Load data into the table
        print("6. Truncating table before loading data", end="")
        run_sql(f"truncate table {table_name}", io=False, timeout=0)
        print(". . . . . . . . . . . . . . . . . . . . . . . ~ Done.")
        print("7: Loading CSV data into the table", end="")
        hdfs_file_path = f"/srv/smartcare/calc_input/cn/{new_file_name}"
        beeline_command = f"LOAD DATA INPATH '{hdfs_file_path}' INTO TABLE {table_name}"
        run_sql(beeline_command, io=False, queue_name=beeline_cfg["DEFAULT_QUEUE"])
        print(" . . . . . . . . . . . . . . . . . . . . . . . . . ~ Done.")

        output_, rows_ = run_sql(f"select * from {table_name} limit 3", io=False, timeout=0)
        print(f"\ntable_name = {table_name}\n{output_} \n{rows_}")

    finally:
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
