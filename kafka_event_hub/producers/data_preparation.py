

class DataPreparation(object):
    """
    This type is used by several producers to fetch and prepare data from different data pipelines
    """

    def lookUpData(self):
        """

        :return: True or False - depends on available data for the pipeline
        :rtype: bool
        """
        return True

    def preProcessData(self):
        """ preprocessing of data being ingested by the producer

        :returns None
        """

    def postProcessData(self):
        """ postprocessing of data being ingested by the producer

        :returns None
        """
