#!/usr/bin/env python3

import sys
import os
import venv
import subprocess

def call(cmd):
    return subprocess.call(cmd, shell=True)

if len(sys.argv) < 2:
    print("Usage: python script.py <entrypoint_file>")
    sys.exit(1)

# Remove .py extension if present
entrypoint = os.path.splitext(sys.argv[1])[0] + ":main"

# Check if we've already re-executed
if not os.environ.get("PEX_SCRIPT_EXECUTED"):
    venv.create("venv", with_pip=True)

    # Set up the environment to use the virtual environment
    venv_path = os.path.abspath("venv")
    bin_path = os.path.join(venv_path, "Scripts" if sys.platform == "win32" else "bin")
    os.environ["VIRTUAL_ENV"] = venv_path
    os.environ["PATH"] = bin_path + os.pathsep + os.environ["PATH"]
    os.environ.pop("PYTHONHOME", None)
    os.environ["PYTHONUNBUFFERED"] = "1"  # Set unbuffered output
    os.environ["PEX_SCRIPT_EXECUTED"] = "1"  # Set flag to indicate re-execution

    # Re-execute the script with the updated environment
    os.execv(sys.executable, [sys.executable, "-u"] + sys.argv)

# Now we're running in the virtual environment
print("Installing nodeenv and pex...")
if call("pip install nodeenv pex") != 0:
    sys.exit("Failed to install nodeenv and pex")

print("Installing Node.js LTS...")
if call("nodeenv -p --node=lts") != 0:
    sys.exit("Failed to install Node.js LTS")

print("Creating PEX package...")
pex_command = f"pex -r requirements.txt -o output.pex --include-tools -e {entrypoint}"

if call(pex_command) != 0:
    sys.exit("Failed to create PEX package")

print("PEX package created successfully: output.pex")