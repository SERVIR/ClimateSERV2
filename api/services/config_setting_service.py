from ..models import Config_Setting
import json, sys

class Config_SettingService():

    @staticmethod
    def get_value(setting_name="", default_or_error_return_value=""):
        ret_val = ""
        try:
            # Filter and Validate.
            setting_name = str(setting_name).strip()
            if (setting_name == ""):
                print("model_Config_Setting.py: get_value: Validation Error: setting_name cannot be blank.")
                ret_val = default_or_error_return_value
                return ret_val

            # Get the first instance that matches this setting name.
            model_instance = Config_Setting.objects.filter(setting_name=setting_name)[0]

            # Get the value, the method of getting the value is based on the datatype.
            setting_value = ""
            setting_data_type = str(model_instance.setting_data_type).strip()
            if (setting_data_type == "JSON"):
                setting_value = json.loads(model_instance.setting_value)
            elif (setting_data_type == "LIST"):
                setting_value = model_instance.setting_value.split(',')
            else:
                # If all the above failed, just grab it as a string
                setting_value = str(model_instance.setting_value).strip()

            # Set the return value.
            ret_val = setting_value

        except:
            sys_error_info = sys.exc_info()
            # human_readable_error = "model_Config_Setting.py: get_value: A Generic Error Occurred when trying to get the value a setting.  (setting_name): " + str(setting_name) + "  Please try again later.  System Error Message: " + str(sys_error_info)
            # print(human_readable_error)
            ret_val = default_or_error_return_value
        return ret_val