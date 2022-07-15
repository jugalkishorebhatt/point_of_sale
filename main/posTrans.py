# https://stackoverflow.com/questions/2447353/getattr-on-a-module

import logging
import traceback
import sys

try:
    import common
except Exception as e:
    from main import common

class PosTrans:

    def __init__(self):
        pass

    def caller(self,CommonFunctions,config):
        """
        Base class to redirect instance calls to necessary modules
        """
        try:
            common.logger.info("Module Name :{0}".format(config.get("module_name", None)))
            common.logger.info("Class Name :{0}".format(config.get("class_name", None)))

            module_name = config.get("module_name", None)
            class_name = config.get("class_name", None)

            if module_name is None and class_name is None:
                raise Exception("PosTran: Please provide correct module and class name")
            
            if module_name and class_name:
                try:
                    module = __import__(module_name)
                except Exception as e:
                    module = __import__(module_name, globals(), locals(), [], 1)
                transform_script = getattr(module,class_name)
                kwargs = {"CommonFunctions":CommonFunctions, "config":config}
                instance = transform_script(**kwargs)

                try:
                    common.logger.info("Calling Dependent Module Name : {0} and Class Name {1}".format(module,class_name))
                    instance.execute()
                except Exception as e:
                    common.logger.error("Loading files from {0} in {1} format failed".format(path,formats))
                    common.logger.error(e)
                    sys.exit(1)
            return True
        except Exception as e:
                    common.logger.error("PosTrans Failed")
                    common.logger.error(e)
                    sys.exit(1)