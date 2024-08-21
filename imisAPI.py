from iMISpy import openAPI
from os import environ
import logging

API = None

def getAPI():
    global API   
    if API is not None:
        return API
    else:
        API = openAPI(environ)
        return API