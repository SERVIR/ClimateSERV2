from django.contrib import admin
from django_json_widget.widgets import JSONEditorWidget

from .models import *


# Register your models here.
@admin.register(EnsembleLayer)
class EnsembleLayerAdmin(admin.ModelAdmin):
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget}
    }
    list_display = ('title', 'url')


admin.site.register(DatasetType)
admin.site.register(DataLayer)
admin.site.register(DataSet)
admin.site.register(HomePage)
admin.site.register(WhatYouCanDo)
