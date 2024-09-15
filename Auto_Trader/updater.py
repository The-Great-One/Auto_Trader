from Auto_Trader import datetime, time
import subprocess

def Updater():
    while True:
        result = subprocess.run(["git", "pull", "--force", "https://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@github.com/The-Great-One/Auto_Trader.git", "main"], stdout=subprocess.PIPE)
        if "send" in str(result):
            print("Updated. Restarting!")
            subprocess.run(["systemctl", "restart", "auto_trade.service"])
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')}] No Updates Detected!")
        time.sleep(60)