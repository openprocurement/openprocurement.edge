# -*- coding: utf-8 -*-
import logging
from time import sleep
from httplib import IncompleteRead
from openprocurement.edge.utils import prepare_couchdb, prepare_couchdb_views


LOGGER = logging.getLogger(__name__)


class CouchDBStorage(object):

    def __init__(self, conf, resource):
        self.config = conf
        user = self.config['storage'].get('user', '')
        password = self.config['storage'].get('password', '')
        if (user and password):
            self.couch_url = "http://{user}:{password}@{host}:{port}".format(
                **self.config['storage'])
        else:
            self.couch_url = "http://{host}:{port}".format(
                **self.config['storage'])
        self.db_name = self.config['storage'].get('db_name', 'bridge_db')
        self.resource = resource
        self.db = prepare_couchdb(self.couch_url, self.db_name, LOGGER)
        db_url = '{}/{}'.format(self.couch_url, self.db_name)
        prepare_couchdb_views(db_url, self.resource, LOGGER)
        self.view_path = '_design/{}/_view/by_dateModified'.format(
            self.resource)

    def get_doc(self, doc_id):
        """
        Trying get doc with doc_id from storage and return doc dict if
        doc exist else None
        :param doc_id:
        :return: dict: or None
        """
        doc = self.db.get(doc_id)
        return doc

    def filter_bulk(self, bulk):
        """
        Receiving list of docs ids and checking existing in storage, return
        dict where key is doc_id and value - dateModified if doc exist
        :param keys: List of docs ids
        :return: dict: key: doc_id, value: dateModified
        """
        sleep_before_retry = 2
        for i in xrange(0, 3):
            try:
                rows = self.db.view(self.view_path, keys=bulk.values())
                resp_dict = {k.id: k.key for k in rows}
                return resp_dict
            except (IncompleteRead, Exception) as e:
                LOGGER.error('Error while send bulk {}'.format(e.message),
                             extra={'MESSAGE_ID': 'exceptions'})
                if i == 2:
                    raise
                sleep(sleep_before_retry)
                sleep_before_retry *= 2

    def save_bulk(self, bulk):
        """
        Save to storage bulk data
        :param bulk: Dict where key: doc_id, value: document
        :return: list: List of tuples with id, success: boolean, reason:
        if success is str: state else exception object
        """
        res = self.db.update(bulk.values())
        results = []
        for success, doc_id, reason in res:
            if success:
                if not reason.startswith('1-'):
                    reason = 'updated'
                else:
                    reason = 'created'
            else:
                if reason.message == u'New doc with oldest dateModified.':
                    success = True
                    reason = 'skipped'
            results.append((success, doc_id, reason))
        return results


def includme(config):
    resource = config.get('resource', 'tenders')
    config['storage_obj'] = CouchDBStorage(config, resource)