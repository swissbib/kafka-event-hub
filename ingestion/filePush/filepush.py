# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2018, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__version__ = "0.2"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """


from ingestion.processor import BaseProcessor


class FilePush(BaseProcessor):
    def __init__(self, appConfig=None):
        BaseProcessor.__init__(self,appConfig)


    def collectItems(self):
        pass
        #BaseProcessor.collectItems(self)
