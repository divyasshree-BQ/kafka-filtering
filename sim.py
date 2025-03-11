import subprocess
import sys
import time
from datetime import datetime

# Configuration
runtime_seconds = 1200  # Adjust this duration according to your requirement
script1_path = "bq.py"
script2_path = "pumplib.py"

def start_script(script_path):
    return subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

def handle_output(process, name):
    for line in process.stdout:
        print(f"[{name}] {line.strip()}", flush=True)

def main():
    print(f"Starting both scripts for {runtime_seconds} seconds")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    script1_process = start_script(script1_path)
    script2_process = start_script(script2_path)

    print(f"Process 1 started with PID: {script1_process.pid}")
    print(f"Process 2 started with PID: {script2_process.pid}\n")
    print("--- Output will be shown below ---\n")

    # Thread not used here; opting to display outputs sequentially in the main thread for simplicity
    try:
        start_time = time.time()
        while time.time() - start_time < runtime_seconds:
            if script1_process.poll() is not None and script2_process.poll() is not None:
                break
            handle_output(script1_process, "Script 1")
            handle_output(script2_process, "Script 2")
            time.sleep(1)  # Reduce frequency of checks to ease CPU usage

        # Terminate processes if still running
        if script1_process.poll() is None:
            print("\nTerminating Process 1...")
            script1_process.terminate()
        if script2_process.poll() is None:
            print("Terminating Process 2...")
            script2_process.terminate()

        # Wait for processes to terminate
        script1_process.wait(timeout=5)
        script2_process.wait(timeout=5)

        # Final exit codes and cleanup
        print(f"\nProcess 1 exit code: {script1_process.returncode}")
        print(f"Process 2 exit code: {script2_process.returncode}")
        print("\nBoth processes have completed.")
        print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    except KeyboardInterrupt:
        print("\nUser interrupted. Terminating processes...")
        if script1_process.poll() is None:
            script1_process.terminate()
        if script2_process.poll() is None:
            script2_process.terminate()
        print("Processes terminated.")

if __name__ == "__main__":
    main()
