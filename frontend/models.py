from django.db import models


class DatasetType(models.Model):
    """Model representing a dataset type"""
    dataset_type = models.CharField(max_length=200, help_text='Type of dataset (currently Model or Observation)')

    def __str__(self):
        return f"{self.dataset_type}"


class DataSet(models.Model):
    """Model representing a dataset for the entire application"""
    short_name = models.CharField(max_length=200, help_text='Enter a short name to identify the dataset', default="Enter-Name")
    heading = models.CharField(max_length=200, help_text='Enter heading to display on home page when featured')
    summary = models.TextField(help_text="Enter summary to display on home page when featured")
    dataset_image = models.ImageField(upload_to='images/', default=None, null=True, blank=True)
    image_alt = models.CharField(max_length=200, help_text="Enter alt text to display on mouse over of feature image")
    #image_src = models.CharField(max_length=200, help_text="trying to remove this", default="")

    button_variable = models.CharField(max_length=200, help_text='Enter variable to pass to map application which '
                                                                 'will enable the selected data type on the map')
    button_text = models.CharField(max_length=200, help_text='Enter text of button to access data on map')
    metadata_id = models.CharField(max_length=200, help_text='Enter metadata id from GeoNetwork')
    featured = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.short_name}"


class DataLayer(models.Model):
    """Model representing a data layer for the map"""
    title = models.CharField(max_length=200, help_text='Enter a title which will display in the layer list on the map '
                                                       'application')
    url = models.TextField(help_text="Enter url to the TDS WMS service")
    attribution = models.TextField(help_text="Enter data attribution to display in map UI")
    layers = models.CharField(max_length=200, help_text='Enter layer names from the WMS to display')
    default_style = models.CharField(max_length=200, help_text='Enter default style to use for WMS to display')
    default_color_range = models.CharField(max_length=200, help_text='Enter default color range to use for WMS to '
                                                                     'display')
    ui_id = models.CharField(max_length=200, help_text='ID for the UI to use and access in javascript')
    dataset_type = models.ForeignKey(DatasetType, on_delete=models.CASCADE, related_name="datatype")
    dataset_id = models.ForeignKey(DataSet, on_delete=models.CASCADE, related_name="dataset")

    def __str__(self):
        return f"{self.title}"
