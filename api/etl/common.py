# UPDATED NOTE: - Create a standardized way to read the event function_response['is_error'] function_response[
# 'human_readable_activity_description_event']    # If this is just a regular activity, the 'text' goes here
# function_response['human_readable_activity_description_error']    # If there is an error, use this field to hold
# the message (and system info if there is any) function_response['function_name'] function_response['class_name']
# function_response['????']

import json


class common():
    @staticmethod
    def get_function_response_object(class_name="UNSET", function_name="UNSET", is_error=False,
                                     event_description="UNSET", error_description="UNSET", detail_state_info={}):
        return {'class_name': str(class_name).strip(), 'function_name': str(function_name).strip(),
                   'is_error': is_error, 'event_description': str(event_description).strip(),
                   'error_description': str(error_description).strip(), 'detail_state_info': json.dumps(
                detail_state_info)}
