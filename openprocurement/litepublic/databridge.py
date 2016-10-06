from gevent import monkey
monkey.patch_all()

try:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

import logging
import logging.config
import os
import argparse
from yaml import load
from urlparse import urljoin
from couchdb import Database, Session
from openprocurement_client.sync import get_tenders
from openprocurement_client.client import TendersClient

logger = logging.getLogger(__name__)


class LitePublicDataBridge(object):

    """Lite Public Bridge"""

    def __init__(self, config):
        super(LitePublicDataBridge, self).__init__()
        self.config = config
        self.api_host = self.config_get('tenders_api_server')
        self.api_version = self.config_get('tenders_api_version')

        self.client = TendersClient(host_url=self.api_host,
            api_version=self.api_version, key=''
        )

        self.couch_url = urljoin(
            self.config_get('couch_url'),
            self.config_get('public_db')
        )
        self.db = Database(self.couch_url,
                           session=Session(retry_delays=range(10)))

    def config_get(self, name):
        return self.config.get('main').get(name)

    def get_teders_list(self):
        for item in get_tenders(host=self.api_host,
                                version=self.api_version,
                                key='', extra_params={'mode': '_all_'}):
            yield (item["id"], item["dateModified"])

    def save_tender_in_db(self, tender_id, date_modified):
        tender_doc = self.db.get(tender_id)
        if tender_doc:
            if tender_doc['dateModified'] == date_modified:
                return
        tender = self.client.get_tender(tender_id).get('data')
        if tender:
            tender['_id'] = tender_id
            tender['doc_type'] = 'Tender'
            if tender_doc:
                tender['_rev'] = tender_doc['_rev']
                logger.info('Update tender {} '.format(tender_id))
            else:
                logger.info('Save tender {} '.format(tender_id))
            try:
                self.db.save(tender)
            except Exception as e:
                logger.info('Saving tender {} fail with error {}'.format(tender_id, e.message),
                    extra={'MESSAGE_ID': 'lite_public_bridge_fail_save_in_db'})
        else:
            logger.info('Tender {} not found'.format(tender_id))

    def run(self):
        logger.info('Start Lite Public Bridge',
                    extra={'MESSAGE_ID': 'lite_public_bridge_start_bridge'})
        logger.info('Start data sync...',
                    extra={'MESSAGE_ID': 'lite_public_bridge__data_sync'})
        for tender_id, date_modified in self.get_teders_list():
            self.save_tender_in_db(tender_id, date_modified)


def main():
    parser = argparse.ArgumentParser(description='---- Lite Public Bridge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        LitePublicDataBridge(config).run()


##############################################################

if __name__ == "__main__":
    main()
