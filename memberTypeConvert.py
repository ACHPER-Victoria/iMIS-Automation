import logging
from iMISpy import helpers
from imisAPI import getAPI
import azure.functions as func
import json

def convertType(query, q: func.Out[str]):
    logging.info("Starting convert type...")
    api = getAPI()
    # build list of IDs to nuke:
    toNMIDs = []
    for item in api.apiIterator("query", [["QueryName", query]]):
        # Change all these results to NM Non-member
        toNMIDs.append(item["ID"])
    for id in toNMIDs:
        q.set(json.dumps({"task": "convert", "data": id}))

def convertProcess(iMISID):
    api = getAPI()
    pobj = api.getContact(iMISID)
    helpers.genericProp(pobj, "CustomerTypeCode", "NM", "AdditionalAttributes")
    api.updateContact(pobj, iMISID)
