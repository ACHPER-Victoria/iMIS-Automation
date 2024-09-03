import json
import logging
from os import environ

import azure.functions as func
from azure.storage.queue import QueueClient, BinaryBase64EncodePolicy

from consecutiveJoinDate import processConsecutiveMembers, processConsecutiveMember
from memberTypeConvert import convertType, convertProcess

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

TASKMAP = {
    "consec": processConsecutiveMember,
    "convert": convertProcess
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

# 10 40 4 * * *
@app.function_name(name="schedulefunc")
@app.timer_trigger(schedule="%ScheduleAppSetting%", arg_name="sch")
@app.queue_output(arg_name="q", queue_name="task", connection="")
def schedulefunc(sch: func.TimerRequest, q: func.Out[str]) -> None:
    IQAQUERY = environ.get("IQAQUERY")
    if IQAQUERY: convertType(IQAQUERY, q)
    if environ.get("SETTINGSBUSINESSOBJECT"): processConsecutiveMembers()

@app.route(route="process")
@app.queue_output(arg_name="q", queue_name="task", connection="AzureWebJobsStorage")
def http_trigger(req: func.HttpRequest, q: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    qc = QueueClient.from_connection_string(environ.get("AzureWebJobsStorage"), "task")
    mep = BinaryBase64EncodePolicy()
    qc.message_encode_policy = mep

    req_body = {}
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    
    if "doconseq" in req_body:
        if req_body["doconseq"] == "all":
            properties = qc.get_queue_properties()
            if properties.approximate_message_count == 0:
                processConsecutiveMembers()
                return func.HttpResponse("Started processing all Consec members.")
            else:
                return func.HttpResponse("Busy, please wait until no more remaining items.")
        else:
            q.set(json.dumps({
                "task": "consec", 
                "data": { "id": req_body["doconseq"], "origjoin": None }
            }))
            return func.HttpResponse(f"Submitted {req_body['doconseq']}.")
    if "status" in req_body:
        properties = qc.get_queue_properties()
        return func.HttpResponse(f"{properties.approximate_message_count}")
    return func.HttpResponse("Done.")
