import datetime
import json
import logging
from os import environ

import azure.functions as func

from consecutiveJoinDate import processConsecutiveMembers, processConsecutiveMember
from memberTypeConvert import convertType

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

TASKMAP = {
    "consec": processConsecutiveMember,
    "convert": convertType
}

@app.function_name(name="QueueProcess")
@app.queue_trigger(arg_name="q", queue_name="task", connection="")  # Queue trigger
def process_queue(q: func.QueueMessage) -> None:
    taskdata = json.loads(q.get_body())
    logging.info('Processing: %s', taskdata)
    task = taskdata["task"]
    data = taskdata["data"]
    if task in TASKMAP: TASKMAP[task](data)
    else: logging.error("Task (%s) not found.", task)
    return

@app.function_name(name="schedulefunc")
@app.timer_trigger(schedule="10 7 2 * * *", arg_name="sch")
@app.queue_output(arg_name="q", queue_name="task", connection="")
def schedulefunc(sch: func.TimerRequest, q: func.Out[str]) -> None:
    IQAQUERY = environ.get("IQAQUERY")
    if IQAQUERY: convertType(IQAQUERY, q)
    if environ.get("CONSECJOINMEMBERS"): processConsecutiveMembers()

@app.route(route="http_trigger")
@app.queue_output(arg_name="q", queue_name="task", connection="")
def http_trigger(req: func.HttpRequest, q: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    imisID = None
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        imisID = req_body.get('imisID')
    if imisID:
        q.set(json.dumps({
            "task": "consec", 
            "data": {"id": imisID, "origjoin": None
            }
        }))
        return func.HttpResponse(f"Submitted {imisID}.")
    else:
        processConsecutiveMembers()
        return func.HttpResponse("Missing imisID", status_code=400)