from Auto_Trader.my_secrets import GITHUB_PAT
import logging
import subprocess
import sys
import time

logger = logging.getLogger("Auto_Trade_Logger")


def Updater():
    while True:
        result = subprocess.run(
            [
                "git",
                "pull",
                "--ff-only",
                f"https://{GITHUB_PAT}@github.com/The-Great-One/Auto_Trader.git",
                "main",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        output = f"{result.stdout}\n{result.stderr}"
        if result.returncode != 0:
            logger.error("Updater git pull failed: %s", output.strip())
        elif "Already up to date." not in output:
            logger.warning("Updated. Restarting!")
            sys.exit(1)
        time.sleep(60)
