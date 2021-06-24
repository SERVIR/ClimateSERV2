from ..models import ETL_Dataset
from ..serializers import ETL_DatasetSerializer

class ETL_DatasetService():

    @staticmethod
    def is_datasetname_avalaible(input__datasetname):
        retBool = True
        try:
            existing_auth_user = ETL_Dataset.objects.filter(dataset_name=str(input__datasetname).strip())[0]
            retBool = False
        except:
            retBool = True
        return retBool

    @staticmethod
    def create_etl_dataset_from_datasetname_only(input__datasetname, created_by="create_dataset_from_datasetname_only"):
        ret_did_create_dataset = False
        ret_Dataset_UUID = ""
        try:
            new_ETL_Dataset = ETL_Dataset()
            new_ETL_Dataset.dataset_name = str(input__datasetname).strip()
            new_ETL_Dataset.created_by = str(created_by).strip()
            new_ETL_Dataset.save()
            ret_Dataset_UUID = new_ETL_Dataset.uuid
            ret_did_create_dataset = True
        except:
            ret_Dataset_UUID = ""
            ret_did_create_dataset = False
        return ret_did_create_dataset, ret_Dataset_UUID

    @staticmethod
    def get_all_etl_datasets_preview_list():
        ret_List = []
        try:
            all_datasets = ETL_Dataset.objects.all()
            for current_dataset in all_datasets:
                ret_List.append(ETL_DatasetSerializer(current_dataset).data)
        except:
            ret_List = []
        return ret_List

    @staticmethod
    def get_all_subtypes_as_string_array():
        return {"subtypes": list(ETL_Dataset.objects.all().values_list('dataset_subtype'))}
