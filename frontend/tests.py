from django.test import TestCase
from .models import *
from api.models import Parameters


# Create your tests here.
class ModelTesting(TestCase):

    def setUp(self):
        self.DatasetType = DatasetType.objects.create(dataset_type="Observation")
        self.Parameters = Parameters.objects.create()

    def test_DatasetType_Model(self):
        """Checking to make sure DatasetType is properly Created"""
        d = self.DatasetType
        self.assertTrue(isinstance(d, DatasetType))
        self.assertEqual(str(d), d.dataset_type)
        self.assertEqual(d.__str__(), d.dataset_type)

    def test_Parameters_Model(self):
        """Checking to make sure Parameters are properly initialized"""
        p = self.Parameters
        self.assertTrue(p.logfilepath == "/cserv2/tmp/")


