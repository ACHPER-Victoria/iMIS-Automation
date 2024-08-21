from os import environ
import logging
import datetime
from typing import List
from iMISpy import iMISAPI
from iMISpy.helpers import genericProp
from imisAPI import getAPI
import requests
import json
from azure.storage.queue import QueueClient, BinaryBase64EncodePolicy
import base64

DATASTRUCT = {
    "$type": "Asi.Soa.Core.DataContracts.GenericEntityData, Asi.Contracts",
    "EntityTypeName": "_BO_NAME",
    "PrimaryParentEntityTypeName": "Party",
    "Identity": {
        "$type": "Asi.Soa.Core.DataContracts.IdentityData, Asi.Contracts",
        "EntityTypeName": "_BO_NAME",
    },
    "PrimaryParentIdentity": {
        "$type": "Asi.Soa.Core.DataContracts.IdentityData, Asi.Contracts",
        "EntityTypeName": "Party",
    },
    "Properties": {
        "$type": "Asi.Soa.Core.DataContracts.GenericPropertyDataCollection, Asi.Contracts",
        "$values": [
            {
                "$type": "Asi.Soa.Core.DataContracts.GenericPropertyData, Asi.Contracts",
                "Name": "ID",
                "Value": "_"
            },
            {
                "$type": "Asi.Soa.Core.DataContracts.GenericPropertyData, Asi.Contracts",
                "Name": "_PROPNAME",
                "Value": "PE"
            }
        ]
    }
}

class HistoryItem:
    def __init__(self, entry):
        self.entry = entry
        self.propertyName = entry["Changes"]["$values"][0]["PropertyName"]
        try:
            self.changedDate = datetime.datetime.strptime(entry["ChangeDate"], "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            self.changedDate = datetime.datetime.strptime(entry["ChangeDate"], "%Y-%m-%dT%H:%M:%S")
        self.originalValue = entry["Changes"]["$values"][0]["OriginalValue"].replace(" - Merged", "")
        self.newValue = entry["Changes"]["$values"][0]["NewValue"].replace(" - Merged", "")

def buildHistoryList(api: iMISAPI, imisID) -> List[HistoryItem]:
    l = []
    # because of iMIS bug, we cannot use Party/ID/changelog, so use the ChangeLog endpoint directly
    # /api/ChangeLog?IdentityEntityTypeName=Party&IdentityIdentityElement=33276
    # I wonder if we can do fancy queries using the endpoint directly...
    for hi in api.apiIterator(f"/api/ChangeLog?IdentityEntityTypeName=Party&IdentityIdentityElement={imisID}", []):
        if "Changes" in hi and len(hi["Changes"]["$values"]) > 1: logging.info(f'Len > 1 ({len(hi["Changes"]["$values"])})')
        if "Changes" in hi and hi["Changes"]["$values"][0]["PropertyName"] == "Name.MEMBER_TYPE":
            l.append(HistoryItem(hi))
    # sort newest to oldest... (it should be that way by default but just to make sure)
    l.sort(key=lambda i: i.changedDate, reverse=True)
    return l

def processConsecutiveMembers():
    logging.info("Starting consectuve members processing...")
    qc = QueueClient.from_queue_url("http://127.0.0.1:10001/devstoreaccount1/task", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
    mep = BinaryBase64EncodePolicy()
    qc.message_encode_policy = mep
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    api = getAPI()
    consectypes = environ.get("CONSECJOINMEMBERS") # format should be like: VICF|VICR
    # iterate over membertypes
    count = 0
    for member in api.apiIterator("/api/Party", [["CustomerTypeCode", f"in:{consectypes}"]]):
        #if count >= 20: break
        qc.send_message(mep.encode(content=json.dumps({
            "task": "consec", 
            "data": {
                "id": member["Id"],
                "origjoin": genericProp(member, "JoinDate", collection="AdditionalAttributes")
            }
        }).encode("utf8")))
        count += 1

def newObj(bo, prop, imisid):
    obj = json.loads(json.dumps(DATASTRUCT))
    obj["EntityTypeName"] = bo
    obj["Identity"]["EntityTypeName"] = bo
    genericProp(obj, "ID", imisid)
    obj["Properties"]["$values"][-1]["Name"] = prop
    return obj

def processConsecutiveMember(taskdata):
    imisID = str(taskdata["id"])
    origjoin = taskdata["origjoin"]
    api = getAPI()
    contact = None
    if origjoin is None:
        # fetch join data from party endpoint since it wasn't provided
        contact = api.getContact(imisID)
        origjoin = genericProp(contact, "JoinDate", collection="AdditionalAttributes")
    if origjoin.startswith("0001"): origjoin = None
    # get allow lapse dates, YYYY-MM-DD comma separated
    allowlapse = environ.get("ALLOWLAPSEDATES","")
    ignorelist = []
    for ignore in environ.get("IGNORESINGLEEXPIRY","").split(","):
        iid, date = ignore.split("|")
        if imisID == iid: ignorelist.append(datetime.datetime.strptime(date, "%Y-%m-%d").date())
    consectypes = environ.get("CONSECJOINMEMBERS").split("|")
    sincebo = environ.get("SINCEBUSINESSOBJECT")
    sinceprop = environ.get("SINCEPROPERTY")
    maxduration = int(environ.get("MAXDURATION", 1))
    allowlapsedates = []
    allowtracking = [] # track whether we entered the allow period as a member...
    currentlymember = True # track whether we are currently a member as we move through the history
    # format: startdate1|enddate1,startdate2|enddate2
    for y in allowlapse.split(","):
        allowlapsedates.append([datetime.datetime.strptime(x, "%Y-%m-%d") for x in y.split("|")])
        allowtracking.append(None)
    # split by | then by comma.
    # put changelog in list...
    history = buildHistoryList(api, imisID)
    now = datetime.datetime.now()
    dateMarker = now
    # then look at the list to figure out oldest consecutive member...
    for hi in history:
        #logging.info(f"hi.newValue: {hi.newValue}, DATE: {hi.changedDate} consec: {hi.newValue in consectypes}")
        if hi.newValue in consectypes: currentlymember = True
        else: 
            if ignorelist:
                ignore = False
                for d in ignorelist:
                    if hi.changedDate.date() == d: ignore = True
                if ignore: currentlymember = True
                else: currentlymember = False
            else: currentlymember = False
        # check if we entered a lapse range, and if we did, pop in if we enteres as a member or not...
        for i,(start,end) in enumerate(allowlapsedates):
            if start < hi.changedDate < end and allowtracking[i] is not None:
                allowtracking[i] = currentlymember
        if hi.newValue in consectypes:
            dateMarker = hi.changedDate
        elif hi.originalValue in consectypes and hi.newValue not in consectypes: # meaning expiry entry
            # first check if current date entry is in exception for ID
            if ignorelist:
                doignore = False
                for d in ignorelist:
                    logging.info(f"{hi.changedDate.date()}, {d}, {hi.changedDate.date() == d}")
                    if hi.changedDate.date() == d: doignore = True
                if doignore:
                    continue
            # check if this changedate is within maxduration
            #logging.info(f"date: {dateMarker - hi.changedDate}, bool: {dateMarker - hi.changedDate < datetime.timedelta(days=30*maxduration)}")
            if dateMarker - hi.changedDate < datetime.timedelta(days=30*maxduration):
                dateMarker = hi.changedDate
            else:
                # *OR* in the allowed lapse date... AND we entered the period as member...
                #loop over allowed ranges
                #logging.info("DOING ELSE?")
                for i, (start,end) in enumerate(allowlapsedates):
                    if start < hi.changedDate < end and allowtracking[i] is True:
                        break # found in allowed, allow if entered the allow period as a member
                else:
                    # did not find in allowed, so do not allow further
                    break
    else:
        # been through entire history... if there was no record of "becoming" 'consecmemtype'
        # then assume the 'original member join date' is the consec member join date
        if origjoin is not None:
            # get original join date
            dateMarker = origjoin
    if origjoin is None:
        # IF origjoin is None it means JoinDate was invalid for some reason, so set it.
        if contact is None: contact = api.getContact(imisID)
        genericProp(contact, "JoinDate", datetime.datetime.strftime(dateMarker, "%Y-%m-%dT00:00:00"), collection="AdditionalAttributes")
        api.updateContact(contact)

    if dateMarker != now:
        # found date so update record...
        obj = None
        was404 = False
        try: obj = api.get(sincebo, imisID)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                was404 = True
                obj = newObj(sincebo, sinceprop, imisID)
        oldval = genericProp(obj, sinceprop)
        newval = dateMarker
        if not isinstance(dateMarker, str):
            newval = datetime.datetime.strftime(dateMarker, "%Y-%m-%dT00:00:00")
        if oldval != newval:
            genericProp(obj, sinceprop, newval)
            if was404: api.post(sincebo, obj)
            else: api.put(sincebo, imisID, obj)
                
                    


