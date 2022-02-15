from django.db import models


class DatasetType(models.Model):
    """Model representing a dataset type"""
    dataset_type = models.CharField(max_length=200, help_text='Type of dataset (currently Model or Observation)')

    def __str__(self):
        return f"{self.dataset_type}"


class DataSet(models.Model):
    """Model representing a dataset for the entire application"""
    short_name = models.CharField(max_length=200, help_text='Enter a short name to identify the dataset',
                                  default="")
    heading = models.CharField(max_length=200, help_text='Enter heading to display on home page when featured')
    feature_subtext = models.TextField(help_text="Enter subtext to display on home page when featured", default="")
    summary = models.TextField(help_text="Enter summary to display in help center card", default="")
    display_image = models.ImageField(default='images/no_data_preview.png', upload_to='images/')
    image_alt = models.CharField(max_length=200, help_text="Enter alt text to display on mouse over of feature image")

    button_variable = models.CharField(max_length=200, help_text='Enter variable to pass to map application which '
                                                                 'will enable the selected data type on the map')
    button_text = models.CharField(max_length=200, help_text='Enter text of button to access data on map')
    # button_color = models.CharField(max_length=200, help_text='Valid hex, RGB, or RGBA color')
    metadata_id = models.CharField(max_length=200, help_text='Enter metadata id from GeoNetwork')
    featured = models.BooleanField(default=False)

    def __str__(self):
        if self.short_name:
            return f"{self.short_name}"
        else:
            return "unnamed"


class DataLayer(models.Model):
    """Model representing a data layer for the map"""

    title = models.CharField(max_length=200, help_text='Enter a title which will display in the layer list on the map '
                                                       'application')
    url = models.TextField(help_text="Enter url to the TDS WMS service")
    attribution = models.TextField(help_text="Enter data attribution to display in map UI")
    layers = models.CharField(max_length=200, help_text='Enter layer names from the WMS to display')
    units = models.CharField(max_length=200, help_text='Enter units to display on chart', default='Dimensionless')
    default_style = models.CharField(max_length=200, help_text='Enter default style to use for WMS to display')
    default_color_range = models.CharField(max_length=200, help_text='Enter default color range to use for WMS to '
                                                                     'display')
    ui_id = models.CharField(max_length=200, help_text='ID for the UI to use and access in javascript')
    dataset_type = models.ForeignKey(DatasetType, on_delete=models.CASCADE, related_name="datatype")
    dataset_id = models.ForeignKey(DataSet, on_delete=models.CASCADE, related_name="dataset")
    api_id = models.CharField(max_length=200, help_text='Enter API ID used to identify this data layer', default="")
    isMultiEnsemble = models.BooleanField(default=False, help_text='This is the main entry to the model ensembles')
    hasVisualization = models.BooleanField(default=True, help_text='Indicates if the layer has wms capabilities.')
    yAxis_Special_Formatting = models.TextField(
        help_text="Only include if your yAxis needs special formatting, this should be a json object with the key "
                  "formatter: function () and the function should return the formatted value", default="",
        blank=True)
    graph_point_Special_Formatting = models.TextField(
        help_text="Only include if your graph point needs special formatting, , this should be a json object with the "
                  "key {pointFormatter: function() nd the function should return the formatted value", default="",
        blank=True)

    def __str__(self):
        return f"{self.title}"


class EnsembleLayer(models.Model):
    """Model representing an ensemble layer for the map"""
    title = models.CharField(max_length=200, help_text='Enter a title which will display in the layer list on the map '
                                                       'application')
    url = models.TextField(help_text="Enter url to the TDS WMS service")
    attribution = models.TextField(help_text="Enter data attribution to display in map UI")
    layers = models.CharField(max_length=200, help_text='Enter layer names from the WMS to display')
    units = models.CharField(max_length=200, help_text='Enter units to display on chart', default='Dimensionless')
    default_style = models.CharField(max_length=200, help_text='Enter default style to use for WMS to display')
    default_color_range = models.CharField(max_length=200, help_text='Enter default color range to use for WMS to '
                                                                     'display')
    ui_id = models.CharField(max_length=200, help_text='Please use lowercase master title + ens + number. IE: nmmeens1')
    master_layer = models.ForeignKey(DataLayer, on_delete=models.CASCADE, related_name="datalayer")
    api_id = models.CharField(max_length=200, help_text='Enter API ID used to identify this data layer', default="")
    sample = models.CharField(max_length=200, help_text='Enter API ID used to identify this data layer', default="")

    def __str__(self):
        name = self.master_layer.title + "_" + self.title
        return f"{name}"


class HomePage(models.Model):
    """Model representing an ensemble layer for the map"""
    hero_title = models.CharField(max_length=200, help_text='Enter a title which will display in the hero image.')
    hero_subtitle = models.CharField(max_length=200, help_text='Enter a subtitle which will display in the hero image.')
    hero_image = models.ImageField(default='images/no_data_preview.png', upload_to='images/')
    intro_text_header = models.CharField(max_length=200, help_text='Header for intro section.')
    intro_text = models.TextField(help_text="Paragraph for intro text")

    def __str__(self):
        name = self.hero_title
        return f"{name}"


class WhatYouCanDo(models.Model):
    title = models.CharField(max_length=200, help_text='Enter a title for the section.')
    body = models.TextField(help_text='Enter the body.')
    display_image = models.ImageField(default='images/no_data_preview.png', upload_to='images/')

    HomePage = models.ForeignKey(HomePage, on_delete=models.CASCADE, related_name="homepage")

    def __str__(self):
        name = self.title
        return f"{name}"
