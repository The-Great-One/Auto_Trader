from Auto_Trader import datetime, time, subprocess, sys, logging
from Auto_Trader.my_secrets import GITHUB_PAT

logger = logging.getLogger("Auto_Trade_Logger")

def Updater():
    while True:
        result = subprocess.run(["git", "pull", "--force", f"https://{GITHUB_PAT}@github.com/The-Great-One/Auto_Trader.git", "main"], stdout=subprocess.PIPE)
        if "send" in str(result):
            logger.warning("Updated. Restarting!")
            sys.exit(1)
        else:
            pass
        time.sleep(60)