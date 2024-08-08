import subprocess, os

def get_package_path(pkg_name: str) -> str:
    """Return the path to the package installed by the pip dependency manager."""
    result = subprocess.run(['pip', 'show', pkg_name], capture_output=True, text=True)    

    def parse_pip_show_output(output):
        info = {}
        for line in output.splitlines():
            if line and ": " in line:
                key, value = line.split(": ", 1)
                info[key.strip()] = value.strip()
        return info
    
    if not result.stdout:
        if result.stderr:
            raise ValueError(result.stderr)
        raise ValueError(f"Could not find package {pkg_name}.")
    
    info = parse_pip_show_output(result.stdout)
    return os.path.join(info["Location"], pkg_name)