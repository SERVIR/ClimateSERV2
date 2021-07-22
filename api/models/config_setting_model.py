from django.db import models
import sys, json

class Config_Setting(models.Model):
    id = models.BigAutoField(primary_key=True)
    setting_name = models.CharField('Setting Name', max_length=250, blank=False, default="UNKNOWN_SETTING", help_text="What is the name of this setting.  These are meant to be unique but not constrained by the database.  If there are any settings with the exact same name, please delete the duplicates.")
    setting_value = models.TextField('Setting Value', default="{}", help_text="Setting Value.  Can be a string, or JSON Object, etc.  If this value is just a string or number only, remove the {} default.")
    setting_data_type = models.CharField('Setting Data Type', max_length=250, blank=False, default="STRING", help_text="Setting Datatype.  This is important.  Choices are: 'STRING', 'LIST', 'JSON'.  If the datatype is not recognized, then STRING is used.  Note: JSON types are loaded with json.loads(<value>).  Note: LIST types are read as comma separated strings.  Something like '1,2,3' would get converted into ['1','2','3'] to be used in the code.  List Items have to be strings, not numbers, and not complex objects.  List Item Type conversion and validation happens at the code where it is needed.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)

    def __str__(self):
        return '{} ({})'.format(self.setting_name, self.setting_data_type)

    class Meta:
        verbose_name = 'Config Setting'
        verbose_name_plural = 'Config Settings'
        ordering = ['setting_name']

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
