from Auto_Trader import datetime, time, subprocess, sys
from Auto_Trader.my_secrets import GITHUB_PAT
def Updater():
    while True:
        result = subprocess.run(["git", "pull", "--force", f"https://{GITHUB_PAT}@github.com/The-Great-One/Auto_Trader.git", "main"], stdout=subprocess.PIPE)
        if "send" in str(result):
            print("Updated. Restarting!")
            sys.exit(1)
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')}] No Updates Detected!")
        time.sleep(60)