import datetime
import logging
from os import environ

import azure.functions as func

from ..iMIS import api, helpers

def main(mytimer: func.TimerRequest) -> None:
    IQAQUERY = environ["IQAQUERY"]

    if mytimer.past_due:
        logging.info('DailyMemberToNonMember is past due!')

    # build list of IDs to nuke:
    toNMIDs = []
    for item in api.IterateQuery(IQAQUERY):
        # Change all these results to NM Non-member
        toNMIDs.append(item["ID"])
    for id in toNMIDs:
        pobj = api.getContact(id)
        helpers.genericProp(pobj, "CustomerTypeCode", "NM", "AdditionalAttributes")
        api.updateContact(pobj, id)
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    logging.info('DailyMemberToNonMember completed at %s', utc_timestamp)
