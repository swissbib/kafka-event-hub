

class DataPreparation(object):
    """
    This type defines a series
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
