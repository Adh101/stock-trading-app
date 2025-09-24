import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print(f"Job started at:",datetime.now())

#Run every minute
#schedule.every().minute.do(basic_job)
#schedule.every().minute.do(run_stock_job)

# Run the job every 5 minutes
schedule.every(3).minutes.do(basic_job)
schedule.every(3).minutes.do(run_stock_job)

#Run on specific time everyday
#schedule.every().day.at("09:30").do(basic_job)
#schedule.every().day.at("09:30").do(run_stock_job)

while True:
    schedule.run_pending()
    time.sleep(1)



