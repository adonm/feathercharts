#!/usr/bin/env python3
from sys import argv, exit
from pathlib import Path
from tempfile import TemporaryDirectory
from zipapp import create_archive
from subprocess import run, check_output
from shutil import copy, copytree

def create_zipapp(script):
    script_path = Path(script)
    
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Copy Python script and install Python dependencies
        (temp_path / "__main__.py").write_bytes(script_path.read_bytes())
        run(f"pip install -r requirements.txt --target {temp_dir} --ignore-installed", shell=True, check=True)
        
        # Install minimal Node.js using volta
        run("volta install node@lts", shell=True, check=True)
        node_path = Path(check_output("volta which node", shell=True).decode().strip())
        npm_path = Path(check_output("volta which npm", shell=True).decode().strip())
        npx_path = Path(check_output("volta which npx", shell=True).decode().strip())
        
        # Copy necessary Node.js files
        node_bin = temp_path / "node" / "bin"
        node_bin.mkdir(parents=True)
        copy(node_path, node_bin / "node")
        copy(npm_path, node_bin / "npm")
        copy(npx_path, node_bin / "npx")
        
        # Copy npm and npx related files
        npm_dir = npm_path.parent.parent
        copytree(npm_dir / "lib", temp_path / "node" / "lib")
        
        # Create __init__.py to set up environment
        (temp_path / "__init__.py").write_text("""
import os
from pathlib import Path
script_dir = Path(__file__).parent
os.environ["PATH"] = f"{script_dir / 'node' / 'bin'}{os.pathsep}{os.environ['PATH']}"
os.environ["NODE_PATH"] = str(script_dir / "node" / "lib" / "node_modules")
        """.strip())
        
        output_path = script_path.with_suffix('.pyz')
        create_archive(temp_dir, output_path, "/usr/bin/env python3", compressed=True)

if __name__ == "__main__":
    if len(argv) != 2 or not Path(argv[1]).is_file() or not Path("requirements.txt").is_file():
        print("Usage: script.py <python_script>\nEnsure script and requirements.txt exist.")
        exit(1)
    
    create_zipapp(argv[1])
    print(f"Zipapp created: {Path(argv[1]).with_suffix('.pyz')}")