# !/usr/bin/python

# https://stackoverflow.com/questions/71934624/creating-a-yaml-file-for-checking-which-userd-id-belongs-to-which-team
# https://www.tutorialspoint.com/pyspark/pyspark_sparkfiles.html


import logging
import traceback
import yaml
import sys
from pyspark.sql.session import SparkSession
from pyspark.files import SparkFiles




logging.basicConfig(
    format = '[[POS_%(filename)s:%(lineno)s :] %(asctime)s, %(msecs)d %(name)s %(levelname)s %(message)s', 
    datefmt = '%Y-%m-%d %H:%M:%S', 
    level = logging.INFO)
    
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
    This class contains the common reusable functions
"""
class CommonFunctions:

    """
    Initialize the class to extend spark function
    """
    def __init__(self,application_name,spark_session=None):

        try:
            self.application_name = application_name
            logger.info("Applicaton: {0} has started".format(application_name))
            CommonFunctions.spark = SparkSession.builder.appName(application_name).getOrCreate() 
        except Exception as e:
            logger.error("Applicaton: {0} has failed".format(application_name))
            logger.error(e)
            sys.exit(1)

    """
    Read files
    """
    def read_files(self,formats,path):
        try:
            logger.info("Loading files from {0} in {1} format".format(path,formats))
            return CommonFunctions.spark.read.format(formats).option("header","true").load(path)
        except Exception as e:
            logger.error("Loading files from {0} in {1} format failed".format(path,formats))
            logger.error(e)
            sys.exit(1)
    
    """
    Load Yaml Files
    """
    def load_yaml(self,yaml_file):
        try:
            logger.info("Loading Yaml file : {0}".format(yaml_file))
            CommonFunctions.spark.sparkContext.addFile(yaml_file)
            yamlfile = SparkFiles.get(yaml_file)
            stream = open(yamlfile,'r')
            yamls = yaml.load(stream)
            return yamls
        except Exception as e:
            logger.error("Loading Yaml file : {0} Failed".format(yaml_file))
            logger.error(e)
            sys.exit(1)    