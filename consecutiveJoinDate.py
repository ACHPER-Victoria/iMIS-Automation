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
import exceptions

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
        if "Changes" in hi and len(hi["Changes"]["$values"]) > 0 and hi["Changes"]["$values"][0]["PropertyName"] == "Name.MEMBER_TYPE":
            l.append(HistoryItem(hi))
    # sort newest to oldest... (it should be that way by default but just to make sure)
    l.sort(key=lambda i: i.changedDate, reverse=True)
    return l

def checkBOs(api: iMISAPI, settings):
    for bo in ('ContiguousBusinessObject', 'LapseAllowBusinessObject', 'IgnoreBusinessObject'):
        try: 
            t = api.get(f"/api/{settings[bo]}", "", params=[["limit", 1]])
            if bo == 'ContiguousBusinessObject':
                # check for property
                if 'ContiguousProperty' not in settings: raise exceptions.SettingsPropertyNotFound(f"ContiguousProperty not found in iMIS settings.")
                if t['Count'] != 1: raise exceptions.SettingsBOWrongType('ContiguousProperty Business object does not appear to be Contact Panel Source, or your database does not have any contacts.')
                if genericProp(t["Items"]["$values"][0], settings['ContiguousProperty']) is None: raise exceptions.SettingsPropertyInBONotFound(f"Property {settings['ContiguousProperty']} doesn't appear to be stored in the {settings[bo]} business object.")
        # TODO: Change the below if iMIS ever changes how not found endpoints react.
        except requests.exceptions.HTTPError: raise exceptions.SettingsBONotFound(f"{bo} ({settings[bo]}) not found.")
def getSettings(api: iMISAPI):
    envbo = environ.get("SETTINGSBUSINESSOBJECT")
    if not envbo: raise exceptions.SettingsEnvBOMissing
    settings = None
    for i in api.apiIterator(f"/api/{envbo}", [["Limit", "1"]]):
        if i is not None: 
            settings = {}
            for s in i['Properties']['$values']:
                settings[s['Name']] = s['Value']['$value'] if isinstance(s['Value'], dict) else s['Value']
        else: break
    if settings is None: raise exceptions.SettingsInImisMissing
    if not settings.get("MEMBERTYPES"): raise exceptions(f"MEMBERTYPES property on {envbo} business object is missing or blank.")
    checkBOs(api, settings)
    allowlapsedates = []
    # process LapseAllow format: startdate1|enddate1,startdate2|enddate2
    for y in api.apiIterator(f"/api/{settings['LapseAllowBusinessObject']}",[]):
        start = genericProp(y, "StartDate")
        if not start: raise exceptions.SettingsLapseMissingOrEmptyStartDate
        end = genericProp(y, "EndDate")
        if not end: raise exceptions.SettingsLapseMissingOrEmptyEndDate
        allowlapsedates.append([datetime.datetime.strptime(start[:10], "%Y-%m-%d").strftime("%Y-%m-%d"), datetime.datetime.strptime(end[:10], "%Y-%m-%d").strftime("%Y-%m-%d")])
    settings["allowlapsedates"] = allowlapsedates
    # process single exceptions
    ignoredates:dict[str,list[str]] = {} # iMISID : []
    for ignore in api.apiIterator(f"/api/{settings['IgnoreBusinessObject']}", []):
        iid = genericProp(ignore, "IMISID")
        if not iid: raise exceptions.SettingsSingleLapseMissingOrEmptyIMISID
        date = genericProp(ignore, "DateToSkip")
        if not date: raise exceptions.SettingsSingleLapseMissingOrEmptyDateToSkip
        if iid not in ignoredates: ignoredates[iid] = []
        ignoredates[iid].append(datetime.datetime.strptime(date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")) # .date()
    settings["ignoredates"] = ignoredates

    return settings

def processConsecutiveMembers():
    logging.info("Starting consectuve members processing...")
    qc = QueueClient.from_connection_string(environ.get("AzureWebJobsStorage"), "task")
    mep = BinaryBase64EncodePolicy()
    qc.message_encode_policy = mep
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    api = getAPI()
    settings = getSettings(api)
    consectypes = settings["MEMBERTYPES"] # format should be like: VICF|VICR
    # iterate over membertypes
    count = 0
    for member in api.apiIterator("/api/Party", [["CustomerTypeCode", f"in:{consectypes}"], ["status", "ne:D"]]):
        #if count >= 20: break
        qc.send_message(mep.encode(content=json.dumps({
            "task": "consec", 
            "data": {
                "id": member["Id"],
                "origjoin": genericProp(member, "JoinDate", collection="AdditionalAttributes"),
                "settings": settings
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

def processConsecutiveMember(taskdata:dict):
    imisID = str(taskdata["id"])
    origjoin:str|None = taskdata["origjoin"]
    settings = taskdata.get("settings")
    api = getAPI()
    if settings is None: settings = getSettings(api)
    contact = None
    if origjoin is None:
        # fetch join data from party endpoint since it wasn't provided
        contact = api.getContact(imisID)
        origjoin = genericProp(contact, "JoinDate", collection="AdditionalAttributes")
    if origjoin.startswith("0001"): origjoin = None
    ignorelist = []
    for i in settings["ignoredates"].get(imisID, []):
        ignorelist.append(datetime.datetime.strptime(i[:10], "%Y-%m-%d").date())
    consectypes = settings["MEMBERTYPES"].split("|")
    sincebo = settings['ContiguousBusinessObject']
    sinceprop = settings['ContiguousProperty']
    maxduration = int(settings.get('MAXDURATION', 1))
    allowlapsedates = []
    # convert the dates in to datetime objects
    for x in settings["allowlapsedates"]:
        allowlapsedates.append([datetime.datetime.strptime(x[0][:10], "%Y-%m-%d"), datetime.datetime.strptime(x[1][:10], "%Y-%m-%d")])
    allowtracking = [] # track whether we entered the allow period as a member...
    currentlymember = True # track whether we are currently a member as we move through the history
    
    # create allowtracking entry for each lapse period
    for i in allowlapsedates:
        allowtracking.append(None)

    # split by | then by comma.
    # put changelog in list...
    history = buildHistoryList(api, imisID)
    now = datetime.datetime.now()
    dateMarker = now
    joinMarker = None
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
        # check if we are in a lapse allow range, and if we are, store in if we exited as a member or not...
        # this means the first time we encounter an allow lapse date field, store the 'currentlymember' value.
        for i,(start,end) in enumerate(allowlapsedates):
            if start < hi.changedDate < end and allowtracking[i] is None:
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
        # special case for merged contacts merging into person who became member on new contact card, but had membership for other state on old card...
        if hi.originalValue == "NM" and hi.newValue in consectypes:
            if hi.changedDate.date() not in ignorelist:
                joinMarker = hi.changedDate
    else:
        # been through entire history... if there was no record of "becoming" 'consecmemtype'
        # then assume the 'original member join date' is the consec member join date
        if origjoin is not None and joinMarker is None:
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
                
                    


