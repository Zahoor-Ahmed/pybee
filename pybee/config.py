import os
import json

# Path to user config file
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".pybee_config.json")

def load_config():
    """
    Load the user configuration from the JSON file.

    Returns:
        dict: Parsed config dictionary.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
    """
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    raise FileNotFoundError("Configuration not found. Please run pybee.set_env() to configure settings.")

# Lazy-access property wrappers for each config section
def SSH_CONFIG():
    return load_config().get("SSH_CONFIG", {})

def BEELINE_CONFIG():
    return load_config().get("BEELINE_CONFIG", {})

def WINSCP_CONFIG():
    return load_config().get("WINSCP_CONFIG", {})
