from django.db import models

class Config_Setting(models.Model):
    id = models.BigAutoField(primary_key=True)
    setting_name = models.CharField('Setting Name', max_length=250, blank=False, default="UNKNOWN_SETTING", help_text="What is the name of this setting.  These are meant to be unique but not constrained by the database.  If there are any settings with the exact same name, please delete the duplicates.")
    setting_value = models.TextField('Setting Value', default="{}", help_text="Setting Value.  Can be a string, or JSON Object, etc.  If this value is just a string or number only, remove the {} default.")
    setting_data_type = models.CharField('Setting Data Type', max_length=250, blank=False, default="STRING", help_text="Setting Datatype.  This is important.  Choices are: 'STRING', 'LIST', 'JSON'.  If the datatype is not recognized, then STRING is used.  Note: JSON types are loaded with json.loads(<value>).  Note: LIST types are read as comma separated strings.  Something like '1,2,3' would get converted into ['1','2','3'] to be used in the code.  List Items have to be strings, not numbers, and not complex objects.  List Item Type conversion and validation happens at the code where it is needed.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)

    def __str__(self):
        return self.setting_name

    class Meta:
        verbose_name = 'Config Setting'
        verbose_name_plural = 'Config Settings'
        ordering = ['setting_name']
