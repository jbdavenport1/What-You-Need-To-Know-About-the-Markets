import subprocess
import sys


SCRIPTS = [
    "build_weekly_market_packet.py",
    "create_market_charts.py",
    "build_weekly_market_docx.py",
    "send_weekly_market_email.py",
]


def run_script(script_name: str) -> None:
    print(f"Running: {script_name}")
    subprocess.run([sys.executable, script_name], check=True)
    print(f"Completed: {script_name}")


def main() -> None:
    for script in SCRIPTS:
        run_script(script)


if __name__ == "__main__":
    main()
