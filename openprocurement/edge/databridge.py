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
import uuid
from yaml import load
from urlparse import urljoin, urlparse
from couchdb import Server, Session, ResourceNotFound
from openprocurement_client.sync import get_resource_items
from openprocurement_client.client import TendersClient as APIClient
import errno
from socket import error
from requests.exceptions import ConnectionError, MissingSchema
import gevent.pool
from gevent import spawn, sleep, idle
from gevent.queue import Queue, Empty

logger = logging.getLogger(__name__)

DEFAULT_API_CLIENTS_COUNT = 5
DEFAULT_QUEUE_TIMEOUT = 3
DEFAULT_WORKERS_COUNT = 3
DEFAULT_RETRY_WORKERS_COUNT = 2
DEFAULT_FILTER_WORKERS_COUNT = 1
DEFALUT_RETRY_TIMEOUT = 5
DEFAULT_WORKERS_SLEEP = 5
DEFAULT_WATCH_INTERVAL = 10
DEFAULT_RETRIES_COUNT = 10
DEFAULT_RESOURCE = 'tenders'
DEFAULT_USER_AGENT = 'op.client'
DEFAULT_COUCHDB_URL = 'http://127.0.0.1:5984'
DEFAULT_COUCHDB_NAME = 'edge_db'

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
        self.api_clients_count = self.config_get('api_clients_count') or DEFAULT_API_CLIENTS_COUNT
        self.workers_count = self.config_get('workers_count') or DEFAULT_WORKERS_COUNT
        self.filter_workers_count = self.config_get('filter_workers_count') or DEFAULT_FILTER_WORKERS_COUNT
        self.retry_workers_count = self.config_get('retry_workers_count') or DEFAULT_RETRY_WORKERS_COUNT
        self.retry_default_timeout = self.config_get('retry_default_timeout') or DEFALUT_RETRY_TIMEOUT
        self.worker_sleep = self.config_get('worker_sleep') or DEFAULT_WORKERS_SLEEP
        self.watch_interval = self.config_get('watch_interval') or DEFAULT_WATCH_INTERVAL
        self.retries_count = self.config_get('retries_count') or DEFAULT_RETRIES_COUNT
        self.queue_timeout = self.config_get('queue_timeout') or DEFAULT_QUEUE_TIMEOUT
        self.user_agent = self.config_get('user_agent') or DEFAULT_USER_AGENT

        self.workers_pool = gevent.pool.Pool(self.workers_count)
        self.filter_workers_pool = gevent.pool.Pool(self.filter_workers_count)
        self.retry_workers_pool = gevent.pool.Pool(self.retry_workers_count)
        self.resource_items_queue = Queue()
        self.api_clients_queue = Queue()
        self.retry_resource_items_queue = Queue()

        self.log_dict = {}
        self.update_seq = 0
        self.doc_count = 0

        if self.api_host != '' and self.api_host is not None:
            api_host = urlparse(self.api_host)
            if api_host.scheme == '' and api_host.netloc == '':
                raise DataBridgeConfigError('Invalid \'tenders_api_server\' url.')
        else:
            raise DataBridgeConfigError('In config dictionary empty or missing'\
                                        ' \'tenders_api_server\'')

        self.couch_url = self.config_get('couch_url') or DEFAULT_COUCHDB_URL
        self.db_name = self.config_get('public_db') or DEFAULT_COUCHDB_NAME

        server = Server(self.couch_url, session=Session(retry_delays=range(10)))

        try:
            if self.db_name not in server:
                self.db = server.create(self.db_name)
            else:
                self.db = server[self.db_name]
        except error as e:
            logger.error('Database error: {}'.format(e.message))
            raise DataBridgeConfigError(e.message)

        db_info = self.db.info()
        self.doc_count = db_info['doc_count']
        self.update_seq = db_info['update_seq']

    def config_get(self, name):
        try:
            return self.config.get('main').get(name)
        except AttributeError as e:
            raise DataBridgeConfigError('In config dictionary missed section' \
                                        ' \'main\'')


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
            logger.debug('Update documents: {}, write documents: {}'.format(
                db_info['update_seq'], db_info['doc_count']
            ))
            db_activity = {
                'doc_count': db_info['doc_count']-self.doc_count,
                'update_seq': db_info['update_seq']-self.update_seq
            }
            self.update_seq = db_info['update_seq']
            self.doc_count = db_info['doc_count']
        except Exception as e:
            logger.error('Error while getting db activity info: {}'.format(
                e.message
            ))
            db_activity = {
                'doc_count': -1,
                'update_seq': -1
            }
        return db_activity

    def add_to_retry_queue(self, resource_item):
        timeout = resource_item.get('timeout') or self.retry_default_timeout
        retries_count = resource_item.get('retries_count') or 0
        resource_item['timeout'] = timeout * 2
        resource_item['retries_count'] = retries_count + 1
        if resource_item['retries_count'] > self.retries_count:
            logger.info('{} {} reached limit retries count {} and' \
                        ' droped from retry_queue.'.format(
                self.resource[:-1].title(), resource_item['id'],
                self.retries_count))
        else:
            spawn(self.retry_resource_items_queue.put,
                  resource_item, timeout=timeout)
            logger.info('Put {} {} to \'retries_queue\''.format(
                self.resource[:-1], resource_item['id']))

    def process_resource_item(self, queue):
        while 1:
            try:
                api_client = self.api_clients_queue.get(timeout=self.queue_timeout)
                logger.info('Got api_client {}'.format(
                    api_client.session.headers['User-Agent']
                ))
            except Empty:
                sleep(self.worker_sleep)
                continue
            try:
                queue_resource_item = queue.get(timeout=self.queue_timeout)
            except Empty:
                self.api_clients_queue.put(api_client)
                sleep(self.worker_sleep)
                continue
            try:
                resource_item = api_client.get_resource_item(
                    queue_resource_item['id']).get('data')  # Resource object from api server
            except Exception as e:
                self.api_clients_queue.put(api_client)
                logger.error('Error while getting resource item from api' \
                             ' server {}: '.format(e.message))
                self.add_to_retry_queue({
                    'id': queue_resource_item['id'],
                    'dateModified': queue_resource_item['dateModified']
                })
                continue
            try:
                resource_item_doc = self.db.get(queue_resource_item['id'])  # Resource object from local db server
            except Exception as e:
                self.api_clients_queue.put(api_client)
                self.add_to_retry_queue({
                    'id': queue_resource_item['id'],
                    'dateModified': queue_resource_item['dateModified']
                })
                logger.error('Error while getting resource item from couchdb: {}'.format(
                    e.message
                ))
                continue
            if resource_item:
                resource_item['_id'] = queue_resource_item['id']
                resource_item['doc_type'] = self.resource[:-1].title()
                if resource_item_doc:
                    resource_item['_rev'] = resource_item_doc['_rev']
                    if resource_item['dateModified'] > resource_item_doc['dateModified']:
                        logger.info('Update {} {} '.format(
                            self.resource[:-1], queue_resource_item['id']))
                    else:
                        self.api_clients_queue.put(api_client)
                        continue
                else:
                    logger.info('Save {} {} '.format(
                        self.resource[:-1], queue_resource_item['id']))
                try:
                    self.api_clients_queue.put(api_client)
                    self.db.save(resource_item)
                except Exception as e:
                    self.api_clients_queue.put(api_client)
                    logger.error('Saving {} {} fail with error {}'.format(
                        self.resource[:-1], queue_resource_item['id'], e.message),
                        extra={'MESSAGE_ID': 'edge_bridge_fail_save_in_db'})
                    self.add_to_retry_queue({
                        'id': queue_resource_item['id'],
                        'dateModified': queue_resource_item['dateModified']})
            else:
                self.api_clients_queue.put(api_client)
                logger.info('{} {} not found'.format(
                    self.resource[:-1].title(), queue_resource_item['id']))


    def fill_api_clients_queue(self):
        while self.api_clients_queue.qsize() < self.api_clients_count:
            client_user_agent = self.user_agent + '/' + uuid.uuid4().hex
            api_client = APIClient(host_url=self.api_host,
                                       user_agent=client_user_agent,
                                       api_version=self.api_version,
                                       key='', resource=self.resource)
            self.api_clients_queue.put(api_client)
            logger.info('Started api_client for server {}'.format(
                api_client.session.headers['User-Agent']))


    def fill_resource_items_queue(self):
        for resource_item in get_resource_items(
            host=self.api_host, version=self.api_version, key='',
            extra_params={'mode': '_all_'}, resource=self.resource,
            retrievers_params=self.retrievers_params):
            self.resource_items_queue.put({
                'id': resource_item['id'],
                'dateModified': resource_item['dateModified']})

    def bridge_stats(self):
        db_activity = self.get_db_activity()
        self.log_dict['resource_items_queue_size'] = self.resource_items_queue.qsize()
        self.log_dict['retry_resource_items_queue_size'] = self.retry_resource_items_queue.qsize()
        self.log_dict['workers_count'] = self.workers_count - self.workers_pool.free_count()
        self.log_dict['filter_workers_count'] = self.filter_workers_count - self.filter_workers_pool.free_count()
        self.log_dict['retry_workers_count'] = self.retry_workers_count - self.retry_workers_pool.free_count()
        self.log_dict['free_api_clients'] = self.api_clients_queue.qsize()
        self.log_dict['write_documents'] = db_activity['doc_count']
        self.log_dict['update_documents'] = db_activity['update_seq']
        logger.info('Resource items queue size: {resource_items_queue_size} \n'\
                    'Retry resource items queue size: {retry_resource_items_queue_size} '\
                    '\nWorkers count: {workers_count} \nFilter workers count: {filter_workers_count}'\
                    ' \nRetry workers count: {retry_workers_count} '\
                    '\nFree api clients: {free_api_clients} \nWrite documents:'\
                    ' {write_documents} \nUpdate documents: {update_documents}'.format(**self.log_dict))

    def gevent_watcher(self):
        self.bridge_stats()
        for i in xrange(0, self.filter_workers_pool.free_count()):
            self.filter_workers_pool.spawn(self.fill_resource_items_queue)
        for i in xrange(0, self.workers_pool.free_count()):
            self.workers_pool.spawn(self.process_resource_item,
                                    self.resource_items_queue)
        for i in xrange(0, self.retry_workers_pool.free_count()):
            self.retry_workers_pool.spawn(self.process_resource_item,
                                          self.retry_resource_items_queue)

    def run(self):
        logger.info('Start Edge Bridge',
                    extra={'MESSAGE_ID': 'edge_bridge_start_bridge'})
        logger.info('Start data sync...',
                    extra={'MESSAGE_ID': 'edge_bridge__data_sync'})
        self.fill_api_clients_queue()
        self.filter_workers_pool.spawn(self.fill_resource_items_queue)
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
