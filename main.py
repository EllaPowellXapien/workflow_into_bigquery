# main.py

import subprocess

def run_script(script_name):
    print(f"Running {script_name}...")
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"{script_name} completed successfully.")
    else:
        print(f"{script_name} failed with error:")
        print(result.stderr)
        exit(result.returncode)

if __name__ == "__main__":
    run_script("updating_csv_to_json.py")
    run_script("updating_json_to_new_csv.py")
    run_script("try_new_updates.py")
