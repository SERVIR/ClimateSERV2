from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def getParameterTypes(request):

@csrf_exempt
def getFeatureLayers(request):

@csrf_exempt
def submitMonthlyRainfallAnalysisRequest(request):

@csrf_exempt
def submitMonthlyGEFSRainfallAnalysisRequest(request):

@csrf_exempt
def submitDataRequest(request):

@csrf_exempt
def getDataRequestProgress(request):

@csrf_exempt
def getDataFromRequest(request):

@csrf_exempt
def getRequiredElements(request):

# Function to return capabilities for a specific dataset
@csrf_exempt
def getCapabilitiesForDataset(request):

# ks refactor 2015 // New API Hook getClimateScenarioInfo
# Note: Each individual capabilities entry is already wrapped as a JSON String
#        This means that those elements need to be individually unwrapped in client code.
#        Here is an example in JavaScript
#    // JavaScript code that sends the api return into an object called 'apiReturnData'
#    var testCapabilities_JSONString = apiReturnData.climate_DataTypeCapabilities[1].current_Capabilities
#    testCapabilities_Unwrapped = JSON.parse(testCapabilities_JSONString)
#    // At this point, 'testCapabilities_Unwrapped' should be a fully unwrapped JavaScript object.
@csrf_exempt
def getClimateScenarioInfo(request):

# getFileForJobID
@csrf_exempt
def getFileForJobID(request):

@csrf_exempt
def scriptAccess(request):

# Logging
# How to access the request stuff.
# logger.info("DEBUG: request: " + str(request))
# req_Data_ToLog = decode_Request_For_Logging(request, get_client_ip(request))

# Handler for getting request log data
# global_CONST_LogToken = "SomeRandomStringThatGoesHere"
# Test url string
# http://localhost:8000/getRequestLogs/?callback=success&sYear=2015&sMonth=10&sDay=01&eYear=2015&eMonth=10&eDay=04&tn=SomeRandomStringThatGoesHere
@csrf_exempt
def getRequestLogs(request):
