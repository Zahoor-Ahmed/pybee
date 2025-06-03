# pybee

🔍 **Introduction**

pybee is a powerful Python library designed to streamline SQL-based analytics and file operations in enterprise big data environments. Built on top of open standards like Beeline (Apache Hive CLI) and SSH, pybee addresses a critical gap faced by analysts working in production environments where Python installations are restricted and SQL is the only supported interface.

🚧 **The Problem**

In many enterprise data platforms, analysts often work with massive distributed databases where data is partitioned across daily,weekly, monthly, and yearly tables. Performing cross-temporal analysis typically involves:

* Manually rewriting and executing repetitive SQL queries across multiple partitions.
* Dealing with rigid SQL scripts that lack the flexibility for dynamic query generation.
* Suffering from slow file transfer speeds when using standard Python libraries for data import/export.

These limitations significantly hinder productivity and scalability in daily data operations.

💡 **The Solution - pybee**

pybee was born out of necessity to bridge the gap between Python’s flexibility and SQL’s static nature — without requiring Python installation on production servers.

Key capabilities include:

* **Dynamic SQL Execution:** Write SQL queries inside Python f-strings, leveraging Python's full syntax to generate dynamic, multi-part queries using loops, conditions, and variables.
* **SSH + Beeline Integration:** Seamlessly execute SQL commands over SSH via Beeline, without modifying existing infrastructure.
* **Fast File Transfers:** Leverages WinSCP’s CLI for significantly faster file uploads and downloads, improving data import/export operations.
* **Jupyter Compatibility:** Includes magic command registration to run SQL in Jupyter notebooks, enhancing interactivity for exploratory analysis.

🚀 **Use Case & Impact**

With pybee, repetitive and manual query execution is replaced by programmable, scalable, and reusable workflows. Data analysts can now automate multi-layered dynamic code & analysis, integrate SQL querying into larger Python-based applications, and improve performance of data operations — all while adhering to production security constraints.

Whether you're building dashboards, data pipelines, or conducting exploratory analysis, pybee empowers you to connect, query, and move data with ease and speed.

## Features

* Execute Hive SQL queries remotely via Beeline over SSH.
* Transfer files to and from local file systems and HDFS.
* Convert Beeline outputs to Pandas DataFrames.
* Upload DataFrames directly as Hive tables.
* Export and import data efficiently.
* Use SQL magics in Jupyter Notebooks so jupyter notebooks can directly accept SQL code for apache spark SQL dialect.
* Date partitioning and utility helpers for scheduling and diagnostics.


---

## Installation

**Recommended:** It is highly recommended to create and activate a virtual environment before installing this library to avoid dependency conflicts with other Python packages.

Use below command to directly install **pybee** from github repository.


```python
pip install git+https://github.com/Zahoor-Ahmed/pybee.git
```


⚠️ Important: After installing pybee, it is mandatory to run the **set_env()** function once to configure your environment. This step sets up the necessary connection settings to enable seamless interaction with your Hive-based database and file system.


To upgrade use:

```python
pip install --upgrade --force-reinstall git+https://github.com/Zahoor-Ahmed/pybee.git
```


---

## Getting Started

### 1. Environment Setup with set_env()

Before using any core functionality of pybee, you must initialize the environment using the set_env() function. This step is mandatory as it sets up the connection credentials and paths required for secure communication with your Hive environment.

❗ Why it's needed:
set_env() creates a hidden configuration file (\~/.pybee_config.json) in your home directory. This file stores essential SSH and Beeline connection settings such as:

* SSH server IP and credentials
* Beeline path, user principal, and Kerberos keytab
* WinSCP configuration for file transfers (on Windows systems)

✅ **How to use set_env():**

```python
import pybee as pb
pb.set_env()
```

This will launch an interactive setup wizard in your terminal to input and save the necessary configuration settings. You only need to do this once unless the environment or credentials change.

### 2. Execute Shell Commands Remotely "run_shell()"

Run any Linux shell command on the remote server via SSH.

```python
import pybee as pb
output = pb.run_shell("ls -l | head -5")
print(output)
```

This will print the first 5 files in the current working directory of the remote server.


---

### 3. Run HDFS Commands

Run HDFS-specific commands (if kerberos authentication is used, run_shell function can handle authentication in backed using provided configuration during initial setup via set_env() function).

```bash
output = pb.run_shell("""
hdfs dfsadmin -report | head;
""")
print(output)
```

This helps fetch quick insights about HDFS capacity and usage.


---

### 4. Run Hive SQL Queries

Run queries through Beeline remotely.

```python
output, rows = pb.run_sql("desc table customers_sample_data", io=1)
```

Displays schema of the `customers_sample_data` table.

Below are all the arguments and their description  that run_sql() function can take:

### `run_sql(sql_query, queue_name=None, io=True, timeout=0, log_enabled=True)`

* **sql_query** (*str*): The SQL query string to be executed.
* **queue_name** (*str, optional*): YARN queue name for job execution. Defaults to the system default if `None`.
* **io** (*bool, optional*): If `True`, prints the output to the console; set to `False` to suppress. Default is `True`.
* **timeout** (*int, optional*): Maximum time in seconds to wait for the query to complete. `0` means no timeout. Default is `0`.
* **log_enabled** (*bool, optional*): If `True`, logs query output to a timestamped text file in the `xlogs` directory. Default is `True`.

**Returns**: A tuple `(output, rows)` where `output` is the formatted query result and `rows` is the number of rows returned.

Note that “sql_query” argument can be provided directly in function call as below:

```python
pb.run_sql("desc table customers_sample_data")
```

OR

It can be a separate formated string (f-string) which can accept any number of variables in it. Infact it can utilize any python functiionality which is available for f-strings in python.  So this enables us to dynamically generate sql queries using python and then passed to run_sql function which will pass it to beeline CLI on server for execution.

Here is an example of dynamically generating a SQL query using Python variables and passing it to `run_sql`

```python
import pybee as pb

table_name = "customers_sample_data"
limit_rows = 10

# Create the SQL query dynamically using an f-string
query = f"""
SELECT customer_id, customer_name, city
FROM {table_name}
WHERE region = 'West'
ORDER BY customer_id
LIMIT {limit_rows}
"""

# Run the SQL query
pb.run_sql(query)
```


---

### 5. Convert Beeline Output to DataFrame

```python
df = pb.text_to_df(output)
display(df)
```

This parses the Beeline-formatted output into a Pandas DataFrame.


---

### 6. Upload Local File to Remote

```python
pb.upload_file("./local/path.csv")
```

Useful for pushing local data to the server for further processing or loading into HDFS.


---

### 7. Download File from Remote to Local

```python
df = pb.download_file("/opt/tmp_files/customers_sample_data.csv")
display(df)
```

Automatically fetches and loads remote CSV into a DataFrame.


---

### 8. Export Hive Table to DataFrame

```python
df = pb.table_to_df("customers_sample_data")
```

Downloads a Hive table to local machine and loads it into a DataFrame.


---

### 9. Download Remote CSV as DataFrame

```python
df = pb.download_df("/opt/tmp_files/customers_sample_data.csv")
```

Shortcut for loading a remote file into a DataFrame in one step.


---

### 10. Upload DataFrame to Hive Table

```python
pb.df_to_table(df)
```

Takes a DataFrame and loads it to Hive via an automated CSV upload and table creation process.


---

### 11. Run SQL in Jupyter via Magic

```sql
%%sql
select * from pybee_test_table limit 10
```

Enables SQL-style syntax in notebooks using the registered SQL magic command. Note that **%%sql** must be the first line in the call for magic to work. This means you must have pybee library imported in a previous cell as well.


---

## Utility Functions

### `alert()`

Triggers a system beep to signal end of long operations.

```python
pb.alert()
```

### `to_sql_inlist()`

Convert a Pandas Series into a SQL `IN` clause string.

```python
ids = pd.Series([1, 2, 3])
pb.to_sql_inlist(ids)  # returns '1','2','3'
```

### `todayx()`

Show list of previous days in epoch format.

```python
pb.todayx(r=5)
```

### `this_monthx()`

Show list of previous months in epoch format.

```python
pb.this_monthx(r=5)
```

### `daypartitions()`

List day partitions for a given epoch month.

```python
pb.daypartitions(648)  # Returns days in Jan 2024
```

### `daypartitions_to_sec()`

Prints start and end seconds for each day in a given epoch month.

```python
pb.daypartitions_to_sec(648)
```

### `export()`

Internal use for exporting query/table outputs.


---

## Meta and Config Helpers

### `confirm_table_size()`

Returns size of Hive table.

```python
pb.confirm_table_size("schema.table_name")
```


---

## 💡 VS Code Snippets for Pybee

To speed up your development workflow with `pybee`, you can add the following custom snippets in Visual Studio Code.

### 🧩 Snippets Included

| Prefix | Description |
|----|----|
| `sql_from_cell` | Run SQL inside a Jupyter cell using `pybee` |
| `for_loop_epoch` | Loop using epoch days and run SQL per day |
| `for_loop_calendar_date` | Loop over calendar date range and run SQL |


---

### 🛠️ How to Install These Snippets in VS Code


1. Open **VS Code**.
2. Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS) to open the Command Palette.
3. Type: `Preferences: Configure User Snippets` and hit Enter.
4. Choose `New Global Snippets file` or `python.json` if you're working in Python.
5. Paste the following JSON content inside the file:

<details> <summary>Click to expand JSON snippet</summary>

```javascript
{
	"SQL inside Cell":
	{
		"prefix": "sql_from_cell",
		"body":
		[
			"import pybee as pb",
			"query = f\"\"\"",
			"",
			"SELECT * from customers_sample_data limit 10",
			"",
			"\"\"\"",
			"",
			"output,rows = pb.run_sql(query)",
			"# export(output)"
		],
		"description": "SQL inside Cell"
	}

	,"for_loop_epoch":
	{
		"prefix": "for_loop_epoch",
		"body":
		[
			"import pybee as pb",
			"from IPython.display import clear_output",
			"import pandas as pd",
			"from datetime import datetime, timedelta",
			"",
			"start = today - 2",
			"end   = today",
			"",
			"df = pd.DataFrame()",
			"for day in range(start,end):",
			"    try:",
			"        query = f\"\"\"",
			"        ------------------------------------------------------------------------------------------",
			"        ",
			"        select customer_id, first_name, last_name, company from customers_sample_data_{$day\\} limit 5",
			"        ",
			"        ------------------------------------------------------------------------------------------",
			"        \"\"\"",
			"        ",
			"        tic = datetime.now()",
			"        output,rows = pb.run_sql(query, io=False)",
			"        toc = datetime.now() - tic",
			"        ",
			"        print(f\"Day: {day} - Execution time: {toc} - {rows.lstrip()}\")",
			"        ",
			"        df_tmp = pb.text_to_df(output)",
			"        df = pd.concat([df, df_tmp], ignore_index=True)",
			"        # df = df.drop_duplicates()           # Uncomment to remove duplicate rows",
			"        # df.index = range(1, len(df) + 1)    # Set new index starting from 1",
			"        # display(df.tail(3))",
			"        # clear_output(wait=True)",

			"    except Exception as e:",
			"        print(f\"ERROR: {str(e)}\")",
		],
		"description": "Loop over a range of days and execute SQL queries for each."
	}

	,"for_loop_calendar_date":
	{
        "prefix": "for_loop_calendar_date",
		"body":
		[
			"import pybee as pb",
			"from IPython.display import clear_output",
			"import pandas as pd",
			"from datetime import datetime, timedelta",
			"",
			"start = \"2025-05-29\"",
			"end   = \"2025-05-31\"",
			"",
			"df = pd.DataFrame()",
			"for day in range(",
			"    (datetime.strptime(start, \"%Y-%m-%d\") - datetime(1970, 1, 1)).days,",
			"    (datetime.strptime(end, \"%Y-%m-%d\") - datetime(1970, 1, 1)).days",
			"):",
			"    try:",
			"        query = f\"\"\"",
			"        ------------------------------------------------------------------------------------------",
			"        ",
			"        select customer_id, first_name, last_name, company from customers_sample_data_{$day\\} limit 5",
			"        ",
			"        ------------------------------------------------------------------------------------------",
			"        \"\"\"",
			"        ",
			"        tic = datetime.now()",
			"        output,rows = pb.run_sql(query, io=False)",
			"        toc = datetime.now() - tic",
			"        ",
			"        print(f\"Day: {day} - Execution time: {toc} - {rows.lstrip()}\")",
			"        ",
			"        df_tmp = pb.text_to_df(output)",
			"        df = pd.concat([df, df_tmp], ignore_index=True)",
			"        # df = df.drop_duplicates()           # Uncomment to remove duplicate rows",
			"        # df.index = range(1, len(df) + 1)    # Set new index starting from 1",
			"        # display(df.tail(3))",
			"        # clear_output(wait=True)",

			"    except Exception as e:",
			"        print(f\"ERROR: {str(e)}\")",
		],
        "description": "For loop iterating over dates to execute SQL queries"
    }

}
```

</details>


6. Save the file.
7. Now, in any Python file or notebook:
   * Type `sql_from_cell` or `for_loop_epoch` and hit `Tab` to expand the snippet.


## License

MIT License

## Author

Developed by @github.com/Zahoor-Ahmed
