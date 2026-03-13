import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def run_step(script_name: str) -> None:
    script_path = BASE_DIR / script_name
    result = subprocess.run([sys.executable, str(script_path)], cwd=str(BASE_DIR), check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> int:
    run_step("build_weekly_market_packet.py")
    run_step("send_weekly_market_email.py")
    print("Weekly market automation run complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
