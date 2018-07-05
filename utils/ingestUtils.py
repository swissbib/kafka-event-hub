import re
from datetime import datetime


class IngestUtils():


    detailedGranularityPattern = re.compile('Thh:mm:ssZ', re.UNICODE | re.DOTALL | re.IGNORECASE)

    @staticmethod
    def getCurrentUTCTimestamp(granularity : str = None ):

        if not granularity is None and  IngestUtils.detailedGranularityPattern.search(granularity):
            cTimeUTC =  datetime.utcnow()
            nTList = [str(cTimeUTC.date()),"T",str(cTimeUTC.hour),":",str(cTimeUTC.minute),":",str(cTimeUTC.second),"Z"]
            return "".join(nTList)
        else:
            #granularity without time => next from should be the current day (think about it again especially with UTC)
            return datetime.utcnow().strftime("%Y-%m-%d")


    @staticmethod
    def getCurrentTimestamp():

        cTime =  datetime.now()
        nTList = [str(cTime.date()),"T",str(cTime.hour),":",str(cTime.minute),":",str(cTime.second),"Z"]
        return "".join(nTList)


    @staticmethod
    def transformFromUntil(value, granularity = 'YYYY-MM-DDThh:mm:ssZ'):
        if IngestUtils.detailedGranularityPattern.search(granularity):
            if (type(value) is str):
                return '{:%Y-%m-%dT%H:%M:%SZ}'.format(
                    datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ'))
            else:
                return '{:%Y-%m-%dT%H:%M:%SZ}'.format(
                    value, '%Y-%m-%dT%H:%M:%SZ')
        else:
            if (type(value) is str):
                return '{:%Y-%m-%d}'.format(
                    datetime.strptime(value, '%Y-%m-%d'))
            else:
                return '{:%Y-%m-%d}'.format(
                    value, '%Y-%m-%d')


#YYYY-MM-DDThh:mm:ssZ
#if __name__ == '__main__':
#
#    print( IngestUtils.setCurrentTimestamp('YYYY-MM-DDThh:mm:ssZ'))
#    print( IngestUtils.setCurrentTimestamp('YYYY-MM-DD'))

