
from os.path import basename
from kafka_event_hub.config.content_collector_config import ContentCollectorConfig as CCFG
import yaml
import re
from  logging import config
import logging


def init_logging(configreppath: str, ccfg = type(CCFG)) -> dict:
    shortcut = re.compile('(^.*?)\..*',re.DOTALL).search(basename(configreppath))
    shortcut_source_name = shortcut.group(1) if shortcut else "default"

    with open(ccfg.logs_config, 'r') as f:
        log_cfg = yaml.safe_load(f.read())
        logging.config.dictConfig(log_cfg)

        return {'source_logger': logging.getLogger(shortcut_source_name),
                'source_logger_summary': logging.getLogger(shortcut_source_name + '_summary'),
                'shortcut_source_name' : shortcut_source_name}

