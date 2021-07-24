from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from . import parameters as params

global_CONST_LogToken = "SomeRandomStringThatGoesHere"
#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s', )
logger = logging.getLogger(__name__)

def readResults(uid):
    '''
    Read a results file from the filesystem based on uuid
    :param uid: unique identifier to find the correct result.
    :rtype: loaded json data from file
    '''
    filename = params.getResultsFilename(uid)
    f = open(filename, "r")
    x = json.load(f)
    f.close()
    f = None
    return x

def processCallBack(request, output, contenttype):
    '''
    Creates the HTTP response loaded with the callback to allow javascript callback. Even for
    Non-same origin output
    :param request: Given request that formulated the intial response
    :param output: dictinoary that contains the response
    :param contenttype: output mime type
    :rtype: response wrapped in call back.
    '''

    # All requests get pumped through this function, so using it as the entry point of the logging all requests
    # Log the Request
    # dataThatWasLogged = set_LogRequest(request, get_client_ip(request))

    if request.method == 'POST':
        try:
            callback = request.POST["callback"]
            return HttpResponse(callback + "(" + output + ")", content_type=contenttype)
        except KeyError:
            return HttpResponse(output)

    if request.method == 'GET':
        try:
            callback = request.GET["callback"]
            return HttpResponse(callback + "(" + output + ")", content_type=contenttype)
        except KeyError:
            return HttpResponse(output)

@csrf_exempt
def getParameterTypes(request):
    '''
    Get a list of all of the parameter types.
    :param request: in coming request, but don't need anything from the request.
    '''
    print("Getting Parameter Types")
    logger.info("Getting Parameter Types")
    #ip = get_client_ip(request)
    return processCallBack(request, json.dumps(params.parameters), "application/javascript")

@csrf_exempt
def getFeatureLayers(request):
    '''
    Get a list of shaoefile feature types supported by the system
    :param request: Given request that formulated the intial response
    :param output: dictinoary that contains the response
    :param contenttype: output mime type
    '''
    logger.info("Getting Feature Layers")
    output = []
    for value in params.shapefileName:
        output.append({'id': value['id'], 'displayName': value['displayName'], 'visible': value['visible']})
    return processCallBack(request, json.dumps(output), "application/javascript")

@csrf_exempt
def getDataFromRequest(request):
    '''
    Get the actual data from the processing request.
    :param request:
    '''
    logger.debug("Getting Data from Request")

    ## Need to encode the data into json and send it.
    try:
        requestid = request.GET["id"]
        logger.debug("Getting Data from Request " + requestid)
        jsonresults = readResults(requestid)
        return processCallBack(request, json.dumps(jsonresults), "application/json")
    except Exception as e:
        logger.warn("problem getting request data for id: " + str(request))
        return processCallBack(request, "need to send id", "application/json")
