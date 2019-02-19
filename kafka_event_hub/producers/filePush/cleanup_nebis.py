
import re
from kafka_event_hub.config import FileNebisScpConfig
from typing import Dict

class CleanupNebis(object):

    def __init__(self, config : type (FileNebisScpConfig)):

        self.config = config
        self.pDeleted = re.compile(self.config.deleted_pattern,re.UNICODE | re.IGNORECASE)
        self.pHeader = re.compile(self.config.header_pattern,re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.p_identifier_key = re.compile(self.config.identifier_key,re.UNICODE | re.DOTALL | re.MULTILINE)
        self.pNebisDeleteRecord = re.compile(self.config.nebis_prepare_deleted,re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.pMarcRecord = re.compile(self.config.nebis_marc_record,re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.pCompleteNebisRecord =  re.compile(self.config.nebis_record_body,re.UNICODE | re.DOTALL)

    def cleanup(self,document: str) -> Dict[str, str]:
        m_key = self.p_identifier_key.search(document)

        if m_key:
            documentkey = m_key.group(1)
            contentSingleFile = "".join(document)
            nebisCompleteRecord = self.completeRecord(contentSingleFile)

            recordDeleted = self.isDeleteRecord(nebisCompleteRecord)
            if recordDeleted:
                return  {'key':documentkey,'cleanDoc': self.prepareDeleteRecord(nebisCompleteRecord)}
            else:
                return {'key':documentkey,'cleanDoc':self.transformNamespace(nebisCompleteRecord)}

        else:
            return {}

    def isDeleteRecord(self ,record):
        header = self.pHeader.search(record)
        deleted = False
        if header:
            tHeader = header.group(0)
            if self.pDeleted.search(tHeader):
                deleted = True

        return deleted

    def transformNamespace(self, document):

        prMarcRecord = self.pMarcRecord.search(document)
        if prMarcRecord:
            return '{GROUP1}{FIX1}{FIX2}{FIX3}{GROUP2}{FIX4}'.format(
                GROUP1=prMarcRecord.group(1),
                FIX1='<marc:record xmlns:marc=\"http://www.loc.gov/MARC21/slim\" \n ',
                FIX2='xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n',
                FIX3='xsi:schemaLocation=\"http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd\">',
                GROUP2=prMarcRecord.group(2),
                FIX4='</marc:record></metadata></record>'
            )
        else:
            raise Exception("transformation of namespace wasn't possible - pattern didn't match")


    def completeRecord(self, document):
        m = self.pCompleteNebisRecord.search(document)
        if m:
            return m.group(1)
        else:
            raise Exception("couldn't find a Nebis Record without syntactic OAI frame")

    def prepareDeleteRecord(self,recordToDelete):

        #nebis redords marked as deleted contain still a metadata section with a lot of rubbish we can throw away
        #look for an example in: notizen/examples.oai/aleph.nebis/nebis.deleted.indent.xml

        spNebisDeletedRecordsToSubstitutePattern = self.pNebisDeleteRecord.search(recordToDelete)
        if spNebisDeletedRecordsToSubstitutePattern:
            return spNebisDeletedRecordsToSubstitutePattern.group(1) + spNebisDeletedRecordsToSubstitutePattern.group(2)
        else:
            #todo: write a message
            return recordToDelete


