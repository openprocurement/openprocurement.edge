# -*- coding: utf-8 -*-
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
from couchdb import Database, Session, ResourceNotFound
from openprocurement_client.sync import get_items
from openprocurement_client.client import TendersClient
import errno
from socket import error
from requests.exceptions import ConnectionError, MissingSchema
import gevent.pool
from gevent import spawn, sleep, idle
from gevent.queue import Queue, Empty

logger = logging.getLogger(__name__)

DEFAULT_CLIENTS_COUNT = 5
DEFAULT_WORKERS_COUNT = 3
DEFAULT_RETRY_WORKERS_COUNT = 2
DEFAULT_FILTER_WORKERS_COUNT = 1
DEFALUT_RETRY_TIMEOUT = 5
DEFAULT_WORKERS_SLEEP = 5
DEFAULT_WATCH_INTERVAL = 10
DEFAULT_RETRIES_COUNT = 10
DEFAULT_RESOURCE = 'tenders'

class DataBridgeConfigError(Exception):
    pass

class EdgeDataBridge(object):

    """Edge Bridge"""

    def __init__(self, config):
        super(EdgeDataBridge, self).__init__()
        self.config = config
        self.api_host = self.config_get('tenders_api_server')
        self.api_version = self.config_get('tenders_api_version')
        self.resource = self.config_get('resource') or DEFAULT_RESOURCE
        self.retrievers_params = self.config_get('retrievers_params')
        self.clients_count = self.config_get('clients_count') or DEFAULT_CLIENTS_COUNT
        self.workers_count = self.config_get('workers_count') or DEFAULT_WORKERS_COUNT
        self.filter_workers_count = self.config_get('filter_workers_count') or DEFAULT_FILTER_WORKERS_COUNT
        self.retry_workers_count = self.config_get('retry_workers_count') or DEFAULT_RETRY_WORKERS_COUNT
        self.retry_default_timeout = self.config_get('retry_default_timeout') or DEFALUT_RETRY_TIMEOUT
        self.worker_sleep = self.config_get('worker_sleep') or DEFAULT_WORKERS_SLEEP
        self.watch_interval = self.config_get('watch_interval') or DEFAULT_WATCH_INTERVAL
        self.retries_count = self.config_get('retries_count') or DEFAULT_RETRIES_COUNT

        self.workers_pool = gevent.pool.Pool(self.workers_count)
        self.filter_workers_pool = gevent.pool.Pool(self.filter_workers_count)
        self.retry_workers_pool = gevent.pool.Pool(self.retry_workers_count)
        self.items_queue = Queue()
        self.clients_queue = Queue()
        self.retry_items_queue = Queue()

        self.servers_id = []
        self.log_dict = {}
        self.update_seq = 0
        self.doc_count = 0

        try:
            self.client = TendersClient(host_url=self.api_host,
                api_version=self.api_version, key='', resource=self.resource
            )
        except MissingSchema:
            raise DataBridgeConfigError('In config dictionary empty or missing \'tenders_api_server\'')
        except ConnectionError as e:
            raise e

        self.couch_url = urljoin(
            self.config_get('couch_url'),
            self.config_get('public_db')
        )
        self.db = Database(self.couch_url,
                           session=Session(retry_delays=range(10)))
        try:
            db_info = self.db.info()
            self.doc_count = db_info['doc_count']
            self.update_seq = db_info['update_seq']
        except ResourceNotFound:
            error_message = "Database with name '" + self.config_get('public_db') + "' doesn\'t exist"
            raise DataBridgeConfigError(error_message)
        except error as e:
            if e.errno == errno.ECONNREFUSED:
                raise DataBridgeConfigError("Connection refused: 'couch_url' is invalid in config dictionary")
        except AttributeError as e:
            raise DataBridgeConfigError('\'couch_url\' is missed or empty in config dictionary.')
        except KeyError as e:
            if e.message == 'db_name':
                raise DataBridgeConfigError('\'public_db\' name is missed or empty in config dictionary')

    def config_get(self, name):
        try:
            return self.config.get('main').get(name)
        except AttributeError as e:
            raise DataBridgeConfigError('In config dictionary missed section \'main\'')


    def get_items_list(self):
        for item in get_items(host=self.api_host, version=self.api_version,
                              key='', extra_params={'mode': '_all_'},
                              retrievers_params=self.retrievers_params,
                              resource=self.resource):
            yield (item["id"], item["dateModified"])

    def get_db_activity(self):
        """
        Get database activity. Count writed and updated documents.

        :returns:
            db_activity(dict): Extra params of query
            :param:
                doc_count(int): Count writed documents from previous check
                update_seq(int): Count updated documents from previous check

        """
        try:
            db_info = self.db.info()
            db_activity = {
                'doc_count': db_info['doc_count']-self.doc_count,
                'update_seq': db_info['update_seq']-self.update_seq
            }
            self.update_seq = db_info['update_seq']
            self.doc_count = db_info['doc_count']
        except Exception as e:
            print e
            db_activity = {
                'doc_count': -1,
                'update_seq': -1
            }
        return db_activity

    def add_to_retry_queue(self, item):
        """
        Add item to retry queue.

        :param:
            item(dict): Extra params of query
            :param:
                item_id(str): Item id
                dateModified(datetime): Date modefied item
                timeout(int): Seconds to lock item in retry queue mulitplied on 2
                retries_count(int): Count retry to execute item

        """
        self.retry_items_queue.put({
            'item_id': item['item_id'],
            'dateModified': item['dateModified'],
            'timeout': item['timeout'] * 2,
            'retries_count': item['retries_count'] + 1
            }, timeout=item['timeout'] * 2)
        logger.info('Put {} {} to \'retries_queue\''.format(
            self.resource[:-1], item['item_id']
        ))

    def items_filter(self, item_id, date_modified):
        """
        Filter for items. Filtered by date_modified and existing document.

        :param:
            item_id(str): Item id
            dateModified(datetime): Date modefied item
        :returns:
            Put to queue filtered item.

        """
        try:
            item_doc = self.db.get(item_id)
        except Exception:
            self.clients_queue.put(client)
            self.add_to_retry_queue({
                'item_id': item_id,
                'dateModified': date_modified,
                'timeout': self.retry_default_timeout,
                'retries_count': 1
            }, self.retry_default_timeout)
        if item_doc:
            if item_doc['dateModified'] != date_modified:
                self.items_queue.put((item_id, date_modified, item_doc['_rev']))
        else:
            self.items_queue.put((item_id, date_modified, None))

    def save_item_in_db(self):
        while 1:
            try:
                client = self.clients_queue.get(timeout=0)
                logger.info('Got client {}'.format(
                    client.session.cookies['AWSELB']
                ))
            except Empty:
                sleep(self.worker_sleep)
                continue
            try:
                item_id, date_modified, rev = self.items_queue.get(timeout=0)
            except Empty:
                self.clients_queue.put(client)
                sleep(self.worker_sleep)
                continue
            try:
                item = client.get_tender(item_id).get('data')
            except Exception:
                self.add_to_retry_queue({
                    'item_id': item_id,
                    'dateModified': date_modified,
                    'timeout': self.retry_default_timeout,
                    'retries_count': 1
                }, self.retry_default_timeout)
                self.clients_queue.put(client)
                continue
            if item:
                item['_id'] = item_id
                item['doc_type'] = self.resource[:-1].title()
                if rev is not None:
                    item['_rev'] = rev
                    logger.info('Update {} {} '.format(self.resource[:-1], item_id))
                else:
                    logger.info('Save {} {} '.format(self.resource[:-1], item_id))
                try:
                    self.db.save(item)
                    self.clients_queue.put(client)
                except Exception as e:
                    logger.info('Saving {} {} fail with error {}'.format(self.resource[:-1], item_id, e.message),
                        extra={'MESSAGE_ID': 'edge_bridge_fail_save_in_db'})
                    self.add_to_retry_queue({
                        'item_id': item_id,
                        'dateModified': date_modified,
                        'timeout': self.retry_default_timeout,
                        'retries_count': 1
                    }, self.retry_default_timeout)
                    self.clients_queue.put(client)
            else:
                logger.info('{} {} not found'.format(self.resource[:-1].title(),
                                                     item_id))
                self.clients_queue.put(client)

    def retry_save_item_in_db(self):
        while 1:
            try:
                client = self.clients_queue.get(timeout=0)
                logger.info('Retry saving {} with client {}'.format(
                    self.resource[:-1], client.session.cookies['AWSELB']
                ))
            except Empty:
                sleep(self.worker_sleep)
                continue
            try:
                retry_item = self.retry_items_queue.get(timeout=0)
            except Empty:
                self.clients_queue.put(client)
                sleep(self.worker_sleep)
                continue
            try:
                item = client.get_tender(retry_item['item_id']).get('data')
            except Exception:
                self.clients_queue.put(client)
                if retry_item['retries_count'] >= self.retries_count:
                    logger.info('{} {} reached limit retries count {} and' \
                                ' droped from retry_queue.'.format(
                        self.resource[:-1].title(), retry_item['item_id'],
                        self.retries_count
                    ))
                else:
                    self.add_to_retry_queue(retry_item, retry_item['timeout'])
                continue
            try:
                item_doc = self.db.get(item_id)
            except Exception:
                self.clients_queue.put(client)
                self.add_to_retry_queue(retry_item, retry_item['timeout'])
            if item_doc:
                if item_doc['dateModified'] == date_modified:
                    self.clients_queue.put(client)
                    continue
            if item:
                item['_id'] = retry_item['item_id']
                item['doc_type'] = self.resource[:-1].title()
                if item_doc:
                    item['_rev'] = item_doc['_rev']
                    logger.info('Update {} {} '.format(self.resource[:-1].title(),
                                                       retry_item['item_id']))
                else:
                    logger.info('Save {} {} '.format(self.resource[:-1].title(),
                                                     retry_item['item_id']))
                try:
                    self.db.save(item)
                    self.clients_queue.put(client)
                except Exception as e:
                    logger.info('Saving {} {} fail with error {}'.format(
                        self.resource[:-1], retry_item['tender_id'], e.message
                        ), extra={'MESSAGE_ID': 'edge_bridge_fail_save_in_db'})
                    self.clients_queue.put(client)
                    self.add_to_retry_queue(retry_item, retry_item['timeout'])
            else:
                logger.info('{} {} not found'.format(self.resource[:-1].title(),
                                                     retry_item['item_id']))
                self.clients_queue.put(client)

    def fill_clients_queue(self):
        while len(self.servers_id) < self.clients_count:
            client = TendersClient(host_url=self.api_host,
                api_version=self.api_version, key='', resource=self.resource
            )
            if client.session.cookies['AWSELB'] not in self.servers_id:
                self.servers_id.append(client.session.cookies['AWSELB'])
                self.clients_queue.put(client)
                logger.info('Started client for server {}'.format(
                    client.session.cookies['AWSELB']))
            else:
                del client

    def fill_items_queue(self):
        for item_id, date_modified in self.get_items_list():
            self.items_filter(item_id, date_modified)

    def gevent_watcher(self):
        db_activity = self.get_db_activity()
        self.log_dict['items_queue_size'] = self.items_queue.qsize()
        self.log_dict['workers_count'] = self.workers_count - self.workers_pool.free_count()
        self.log_dict['filter_workers_count'] = self.filter_workers_count - self.filter_workers_pool.free_count()
        self.log_dict['retry_workers_count'] = self.retry_workers_count - self.retry_workers_pool.free_count()
        self.log_dict['free_clients'] = self.clients_queue.qsize()
        self.log_dict['write_documents'] = db_activity['doc_count']
        self.log_dict['update_documents'] = db_activity['update_seq']
        for i in xrange(0, self.workers_pool.free_count()):
            self.workers_pool.spawn(self.save_item_in_db)
        for i in xrange(0, self.retry_workers_pool.free_count()):
            self.retry_workers_pool.spawn(self.retry_save_item_in_db)
        print '================================================================'
        print self.log_dict
        print '================================================================'

    def run(self):
        logger.info('Start Edge Bridge',
                    extra={'MESSAGE_ID': 'edge_bridge_start_bridge'})
        logger.info('Start data sync...',
                    extra={'MESSAGE_ID': 'edge_bridge__data_sync'})
        # self.fill_queue_pool.spawn(self.get_teders_list)
        self.fill_clients_queue()
        self.filter_workers_pool.spawn(self.fill_items_queue)
        while 1:
            self.gevent_watcher()
            sleep(self.watch_interval)


def main():
    parser = argparse.ArgumentParser(description='---- Edge Bridge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        EdgeDataBridge(config).run()


##############################################################

if __name__ == "__main__":
    main()
