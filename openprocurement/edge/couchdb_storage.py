# -*- coding: utf-8 -*-
# from gevent import monkey
# monkey.patch_all()

import logging
import logging.config
import os
from couchdb import Server, Session
from gevent import sleep
from iso8601 import parse_date
from httplib import IncompleteRead
from openprocurement.edge.utils import (
    prepare_couchdb,
    prepare_couchdb_views,
    StorageException
)
from datetime import datetime
from pytz import timezone

TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')

logger = logging.getLogger(__name__)


class CouchDBStorage(object):

    def __init__(self, db_name, couch_url, resource):
        self.db_name = db_name
        self.couch_url = couch_url
        self.resource = resource
        self.db = prepare_couchdb(self.couch_url, self.db_name, logger)
        db_url = self.couch_url + '/' + self.db_name
        prepare_couchdb_views(db_url, self.resource, logger)
        self.server = Server(self.couch_url,
                             session=Session(retry_delays=range(10)))
        self.view_path = '_design/{}/_view/by_dateModified'.format(
            self.resource)

        self.start_time = datetime.now()

    def check(self, queue_resource_item):
        # Try get resource item from local storage
        try:
            # Resource object from local db server
            resource_item_doc = self.db.get(queue_resource_item['id'])
            if (resource_item_doc and
                    resource_item_doc['dateModified'] >=
                    queue_resource_item['dateModified']):
                return True, None
        except Exception as e:
            logger.error('Error while getting resource item from couchdb: '
                         '{}'.format(e.message))
            raise StorageException(e.message)
        if resource_item_doc:
            revision = resource_item_doc.get('_rev')
        else:
            revision = None
        return False, revision

    def filter(self, data):
        sleep_before_retry = 2

        for i in xrange(0, 3):
            try:
                rows = self.db.view(self.view_path, keys=data)
                resp_dict = {k.id: k.key for k in rows}
                return resp_dict
            except (IncompleteRead, Exception) as e:
                logger.error('Error while send bulk {}'.format(e.message),
                             extra={'MESSAGE_ID': 'exceptions'})
                if i == 2:
                    raise e
                sleep(sleep_before_retry)
                sleep_before_retry *= 2

    def save(self, data):
        add_to_retry = []
        try:
            res = self.db.update(data)
            for resource_item in data:
                ts = (datetime.now(TZ) -
                      parse_date(resource_item[
                          'dateModified'])).total_seconds()
                logger.debug('{} {} timeshift is {} sec.'.format(
                    self.resource[:-1], resource_item['id'], ts),
                    extra={'DOCUMENT_TIMESHIFT': ts})
            logger.info('Save bulk {} docs to db.'.format(len(data)))
        except Exception as e:
            logger.error('Error while saving docs: {}'.format(e.message))
            for doc in data:
                add_to_retry.append({'id': doc['id'],
                                     'dateModified': doc['dateModified']})
            return add_to_retry
        for success, doc_id, rev_or_exc in res:
            if success:
                if not rev_or_exc.startswith('1-'):
                    logger.info('Update {} {}'.format(
                        self.resource[:-1], doc_id),
                        extra={'MESSAGE_ID': 'update_documents'})
                else:
                    logger.info('Save {} {}'.format(
                        self.resource[:-1], doc_id),
                        extra={'MESSAGE_ID': 'save_documents'})
                continue
            else:
                if rev_or_exc.message !=\
                        u'New doc with oldest dateModified.':
                    add_to_retry.append({'id': doc_id, 'dateModified': None})
                    logger.error(
                        'Put to retry queue {} {} with reason: '
                        '{}'.format(self.resource[:-1],
                                    doc_id, rev_or_exc.message))
                else:
                    logger.debug('Ignored {} {} with reason: {}'.format(
                        self.resource[:-1], doc_id, rev_or_exc),
                        extra={'MESSAGE_ID': 'skiped'})
                    continue
        return add_to_retry

    def get(self, doc_id):
        doc = self.db.get(doc_id)
        return doc


def includme(config):
    db_name = config.get('main', {}).get('storage', {}).get('db_name')
    couch_url = config.get('main', {}).get('storage', {}).get('couch_url')
    resource = config.get('main', {}).get('resource')
    config['storage_obj'] = CouchDBStorage(db_name, couch_url, resource)
