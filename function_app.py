import logging
import azure.functions as func

from main import run_pipeline

app = func.FunctionApp()

@app.timer_trigger(schedule="0 30 0 * * *", arg_name="myTimer", run_on_startup=True)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    logging.info("Weather pipeline started.")
    run_pipeline()
    logging.info("Weather pipeline finished.")
