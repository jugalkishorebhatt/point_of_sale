# !/usr/bin/python

############################################################################################
# file_name: posTransJob.py
# Objective: Main class 
#
# References:
# https://stackoverflow.com/questions/54531444/valueerror-must-start-with-a-character
# https://stackoverflow.com/questions/37626662/get-yaml-key-value-in-python
#
############################################################################################

import logging
import traceback
import argparse
import common
import posTrans

"""
Main Class to initialize objects and dependent classes
"""
class PosTransJob:

    def __init__(self):
        pass
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config", help="Mandatory File: File Path for the config yaml")

    args = parser.parse_args()
    configfile = args.config

    CommonFunctions = common.CommonFunctions("POS_Transactions")
    
    try:
        if configfile is '':
            common.logger.info("config.yaml not loaded")
        else:
            config = CommonFunctions.load_yaml(configfile)
    except Exception as e:
            common.logger.error("Config loading failed")
            common.logger.error(traceback.print_exec())
            sys.exit(1)

    try:
        pos = posTrans.PosTrans()
        pos.caller(CommonFunctions,config)
    except Exception as e:
            common.logger.error("posTrans caller function failed")
            common.logger.error(traceback.print_exec())
            sys.exit(1)