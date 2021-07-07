# UPDATED NOTE: - Create a standardized way to read the event
# function_response['is_error']
# function_response['human_readable_activity_description_event']    # If this is just a regular activity, the 'text' goes here
# function_response['human_readable_activity_description_error']    # If there is an error, use this field to hold the message (and system info if there is any)
# function_response['function_name']
# function_response['class_name']
# function_response['????']

import json

class common():
    @staticmethod
    def get_function_response_object(class_name="UNSET", function_name="UNSET", is_error=False, event_description="UNSET", error_description="UNSET", detail_state_info={}):
        retObj = {}
        retObj['class_name']        = str(class_name).strip()
        retObj['function_name']     = str(function_name).strip()
        retObj['is_error']          = is_error
        retObj['event_description'] = str(event_description).strip()
        retObj['error_description'] = str(error_description).strip()
        retObj['detail_state_info'] = json.dumps(detail_state_info) #str(detail_state_info).strip()  # additional_json_STR = json.dumps( additional_json )
        return retObj
