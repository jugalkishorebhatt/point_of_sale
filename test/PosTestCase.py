# !/usr/bin/python

############################################################################################
# file_name: PosTestCase.py
# Objective: Unit Test case for POS Application
#
# References:
# https://towardsdatascience.com/the-elephant-in-the-room-how-to-write-pyspark-unit-tests-a5073acabc34
# https://docs.python.org/3/library/unittest.html
# https://stackoverflow.com/questions/1057843/how-can-i-import-a-package-using-import-when-the-package-name-is-only-know
#
############################################################################################

import yaml
from main import common, posTrans
from test.PySparkTest import PySparkTest

class PosTestCase(PySparkTest):

    def setUp(self):
        yaml_file = "/home/dwari/workspace/main/config.yaml"       
        self.CommonFunctions = common.CommonFunctions("POS_Transactions_Test")
        #self.config = yaml.load(open(yaml_file,'r'))
        self.config = self.CommonFunctions.load_yaml(yaml_file)

    def test_load_yaml(self):
        assert self.config is not None

    def test_CommomFunctions(self):
        assert self.CommonFunctions is not None

    def test_read_files(self):
        data = self.CommonFunctions.read_files('csv','/home/dwari/workspace/data/CardBase.csv') 
        self.assertEqual(data.count(),500)

    def test_end_to_end(self):
        pos = posTrans.PosTrans()
        self.assertTrue(pos.caller(self.CommonFunctions,self.config))
