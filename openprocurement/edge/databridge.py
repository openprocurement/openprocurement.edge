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
import psutil
import argparse
import uuid
from yaml import load
from urlparse import urljoin, urlparse
from couchdb import Server, Session, ResourceNotFound
from openprocurement_client.sync import get_resource_items
from openprocurement_client.exceptions import InvalidResponse, RequestFailed
from openprocurement_client.client import TendersClient as APIClient
from openprocurement.edge.collector import LogsCollector
import errno
from socket import error
from requests.exceptions import ConnectionError, MissingSchema
import gevent.pool
from gevent import spawn, sleep, idle
from gevent.queue import Queue, Empty
from datetime import datetime
from .workers import ResourceItemWorker

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_TIMEOUT = 3
DEFAULT_WORKERS_MIN = 1
DEFAULT_WORKERS_MAX = 3
DEFAULT_RETRY_WORKERS_MIN = 1
DEFAULT_RETRY_WORKERS_MAX = 2
DEFAULT_FILTER_WORKERS_COUNT = 1
DEFAULT_RETRY_TIMEOUT = 5
DEFAULT_WORKERS_SLEEP = 5
DEFAULT_WATCH_INTERVAL = 10
DEFAULT_RETRIES_COUNT = 10
DEFAULT_RESOURCE = 'tenders'
DEFAULT_USER_AGENT = 'edge_' + DEFAULT_RESOURCE + '.client'
DEFAULT_COUCHDB_URL = 'http://127.0.0.1:5984'
DEFAULT_COUCHDB_NAME = 'edge_db'
DEFAULT_LOGDB_NAME = 'logs_db'
DEFAULT_RESOURCE_ITEMS_LIMIT = 100
DEFAULT_RESOURCE_ITEMS_QUEUE_SIZE = 102
DEFAULT_RETRY_RESOURCE_ITEMS_QUEUE_SIZE = -1
DEFAULT_WORKERS_INC_THRESHOLD = 90
DEFAULT_WORKERS_DEC_THRESHOLD = 30
DEFAULT_CLIENT_INC_STEP_TIMEOUT = 0.1
DEFAULT_CLIENT_DEC_STEP_TIMEOUT = 0.02
DEFAULT_DROP_THRESHOLD_CLIENT_COOKIES = 2
DEFAULT_QUEUES_CONTROLLER_TIMEOUT = 60


class DataBridgeConfigError(Exception):
    pass


class EdgeDataBridge(object):

    """Edge Bridge"""

    def __init__(self, config):
        super(EdgeDataBridge, self).__init__()
        self.config = config
        self.workers_config = {}
        self.log_dict = {}
        self.api_host = self.config_get('resources_api_server')
        self.api_version = self.config_get('resources_api_version')
        self.retrievers_params = self.config_get('retrievers_params')

        # Workers settings
        self.workers_config['resource'] = self.config_get('resource') or DEFAULT_RESOURCE
        self.workers_config['client_inc_step_timeout'] = self.config_get('client_inc_step_timeout') or DEFAULT_CLIENT_INC_STEP_TIMEOUT
        self.workers_config['client_dec_step_timeout'] = self.config_get('client_dec_step_timeout') or DEFAULT_CLIENT_DEC_STEP_TIMEOUT
        self.workers_config['drop_threshold_client_cookies'] = self.config_get('drop_threshold_client_cookies') or DEFAULT_DROP_THRESHOLD_CLIENT_COOKIES
        self.workers_config['worker_sleep'] = self.config_get('worker_sleep') or DEFAULT_WORKERS_SLEEP
        self.workers_config['retry_default_timeout'] = self.config_get('retry_default_timeout') or DEFAULT_RETRY_TIMEOUT
        self.workers_config['retries_count'] = self.config_get('retries_count') or DEFAULT_RETRIES_COUNT
        self.workers_config['queue_timeout'] = self.config_get('queue_timeout') or DEFAULT_QUEUE_TIMEOUT

        self.workers_inc_threshold = self.config_get('workers_inc_threshold') or DEFAULT_WORKERS_INC_THRESHOLD
        self.workers_dec_threshold = self.config_get('workers_dec_threshold') or DEFAULT_WORKERS_DEC_THRESHOLD
        self.workers_min = self.config_get('workers_min') or DEFAULT_WORKERS_MIN
        self.workers_max = self.config_get('workers_max') or DEFAULT_WORKERS_MAX
        self.workers_pool = gevent.pool.Pool(self.workers_max)

        # Retry workers settings
        self.retry_workers_min = self.config_get('retry_workers_min') or DEFAULT_RETRY_WORKERS_MIN
        self.retry_workers_max = self.config_get('retry_workers_max') or DEFAULT_RETRY_WORKERS_MAX
        self.retry_workers_pool = gevent.pool.Pool(self.retry_workers_max)

        self.retry_resource_items_queue_size = self.config_get('retry_resource_items_queue_size') or DEFAULT_RETRY_RESOURCE_ITEMS_QUEUE_SIZE

        self.filter_workers_count = self.config_get('filter_workers_count') or DEFAULT_FILTER_WORKERS_COUNT
        self.watch_interval = self.config_get('watch_interval') or DEFAULT_WATCH_INTERVAL
        self.user_agent = self.config_get('user_agent') or DEFAULT_USER_AGENT
        self.log_db_name = self.config_get('logs_db') or DEFAULT_LOGDB_NAME
        self.resource_items_queue_size = self.config_get('resource_items_queue_size') or DEFAULT_RESOURCE_ITEMS_QUEUE_SIZE
        self.resource_items_limit = self.config_get('resource_items_limit') or DEFAULT_RESOURCE_ITEMS_LIMIT
        self.queues_controller_timeout = self.config_get('queues_controller_timeout') or DEFAULT_QUEUES_CONTROLLER_TIMEOUT

        self.filter_workers_pool = gevent.pool.Pool(self.filter_workers_count)
        if self.resource_items_queue_size == -1:
            self.resource_items_queue = Queue()
        else:
            self.resource_items_queue = Queue(self.resource_items_queue_size)
        self.api_clients_queue = Queue()
        if self.retry_resource_items_queue_size == -1:
            self.retry_resource_items_queue = Queue()
        else:
            self.retry_resource_items_queue = Queue(self.retry_resource_items_queue_size)

        self.process = psutil.Process(os.getpid())

        # Variables for statistic
        self.log_dict['not_actual_docs_count'] = 0
        self.log_dict['update_documents'] = 0
        self.log_dict['save_documents'] = 0
        self.log_dict['add_to_retry'] = 0
        self.log_dict['droped'] = 0
        self.log_dict['skiped'] = 0
        self.log_dict['add_to_resource_items_queue'] = 0
        self.log_dict['exceptions_count'] = 0
        self.log_dict['not_found_count'] = 0

        if self.api_host != '' and self.api_host is not None:
            api_host = urlparse(self.api_host)
            if api_host.scheme == '' and api_host.netloc == '':
                raise DataBridgeConfigError(
                    'Invalid \'tenders_api_server\' url.')
        else:
            raise DataBridgeConfigError('In config dictionary empty or missing'
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
            raise DataBridgeConfigError(e.strerror)

        collector_config = {
            'main': {
                'storage': 'couchdb',
                'couch_url': self.couch_url,
                'log_db': self.log_db_name
            }
        }
        self.logger = LogsCollector(collector_config)

    def config_get(self, name):
        try:
            return self.config.get('main').get(name)
        except AttributeError as e:
            raise DataBridgeConfigError('In config dictionary missed section'
                                        ' \'main\'')


    def create_api_client(self):
        client_user_agent = self.user_agent + '/' + uuid.uuid4().hex
        timeout = 0
        while 1:
            try:
                api_client = APIClient(host_url=self.api_host,
                                       user_agent=client_user_agent,
                                       api_version=self.api_version,
                                       key='',
                                       resource=self.workers_config['resource'])
                self.api_clients_queue.put({
                    'client': api_client,
                    'request_interval': 0})
                logger.info('Started api_client {}'.format(
                    api_client.session.headers['User-Agent']))
                break
            except RequestFailed as e:
                self.log_dict['exceptions_count'] += 1
                logger.error('Failed start api_client with status code {}'.format(
                    e.status_code
                ))
                timeout += 0.1
                sleep(timeout)

    def fill_api_clients_queue(self):
        while self.api_clients_queue.qsize() < self.workers_min:
            self.create_api_client()

    def fill_resource_items_queue(self):
        for resource_item in get_resource_items(
            host=self.api_host, version=self.api_version, key='',
            extra_params={'mode': '_all_', 'limit': self.resource_items_limit},
            resource=self.workers_config['resource'], retrievers_params=self.retrievers_params):
            if self.resource_items_filter(resource_item['id'],
                                     resource_item['dateModified']):
                self.resource_items_queue.put({
                    'id': resource_item['id'],
                    'dateModified': resource_item['dateModified']})
                self.log_dict['add_to_resource_items_queue'] += 1
            else:
                self.log_dict['skiped'] += 1

    def resource_items_filter(self, r_id, r_date_modified):
        try:
            local_document = self.db.get(r_id)
            if local_document:
                if local_document['dateModified'] < r_date_modified:
                    return True
                else:
                    return False
            else:
                return True
        except Exception as e:
            logger.error('Filter error: Error while getting {} {} from'
                         ' couchdb: {}'.format(self.workers_config['resource'][:-1],
                                               r_id, e.message))
            return True

    def reset_log_counters(self):
        self.log_dict['not_actual_docs_count'] = 0
        self.log_dict['add_to_retry'] = 0
        self.log_dict['droped'] = 0
        self.log_dict['update_documents'] = 0
        self.log_dict['save_documents'] = 0
        self.log_dict['skiped'] = 0
        self.log_dict['not_found_count'] = 0
        self.log_dict['exceptions_count'] = 0
        self.log_dict['add_to_resource_items_queue'] = 0

    def bridge_stats(self):
        return dict(
            time=datetime.now().isoformat(),
            resource_items_queue_size=self.resource_items_queue.qsize(),
            retry_resource_items_queue_size=self.retry_resource_items_queue.qsize(),
            workers_count=self.workers_max - self.workers_pool.free_count(),
            filter_workers_count=self.filter_workers_count - self.filter_workers_pool.free_count(),
            retry_workers_count=self.retry_workers_max - self.retry_workers_pool.free_count(),
            free_api_clients=self.api_clients_queue.qsize(),
            save_documents=self.log_dict['save_documents'],
            update_documents=self.log_dict['update_documents'],
            add_to_retry=self.log_dict['add_to_retry'],
            droped=self.log_dict['droped'],
            skiped=self.log_dict['skiped'],
            rss=self.process.memory_info().rss/1024/1024,
            vms=self.process.memory_info().vms/1024/1024,
            exceptions_count=self.log_dict['exceptions_count'],
            not_found_count=self.log_dict['not_found_count'],
            not_actual_docs_count=self.log_dict['not_actual_docs_count'],
            add_to_resource_items_queue=self.log_dict['add_to_resource_items_queue'],
            resource=self.workers_config['resource']
        )

    def queues_controller(self):
        while True:
            if self.workers_pool.free_count() > 0 and (self.resource_items_queue.qsize() > int((self.resource_items_queue_size / 100) * self.workers_inc_threshold)):
                self.create_api_client()
                w = ResourceItemWorker.spawn(self.api_clients_queue,
                                             self.resource_items_queue,
                                             self.db, self.workers_config,
                                             self.retry_resource_items_queue,
                                             self.log_dict)
                self.workers_pool.add(w)
            elif self.resource_items_queue.qsize() < int((self.resource_items_queue_size / 100) * self.workers_dec_threshold):
                if len(self.workers_pool) > self.workers_min:
                    wi = self.workers_pool.greenlets.pop()
                    wi.shutdown()
            filled_resource_items_queue = int(self.resource_items_queue.qsize()/(self.resource_items_queue_size / 100))
            logger.info('Resource items queue filled on {} %'.format(filled_resource_items_queue))
            filled_retry_resource_items_queue = int(self.retry_resource_items_queue.qsize()/(self.retry_resource_items_queue_size / 100))
            logger.info('Retry resource items queue filled on {} %'.format(filled_retry_resource_items_queue))
            sleep(self.queues_controller_timeout)


    def gevent_watcher(self):
        spawn(self.logger.save, self.bridge_stats())
        self.reset_log_counters()
        for i in xrange(0, self.filter_workers_pool.free_count()):
            self.filter_workers_pool.spawn(self.fill_resource_items_queue)
        if len(self.workers_pool) < self.workers_min:
            for i in xrange(0, (self.workers_min - len(self.workers_pool))):
                w = ResourceItemWorker.spawn(self.api_clients_queue,
                                             self.resource_items_queue,
                                             self.db, self.workers_config,
                                             self.retry_resource_items_queue,
                                             self.log_dict)
                self.workers_pool.add(w)
        if len(self.retry_workers_pool) < self.retry_workers_min:
            for i in xrange(0, self.retry_workers_min - len(self.retry_workers_pool)):
                self.create_api_client()
                w = ResourceItemWorker.spawn(self.api_clients_queue,
                                             self.retry_resource_items_queue,
                                             self.db, self.workers_config,
                                             self.retry_resource_items_queue,
                                             self.log_dict)
                self.retry_workers_pool.add(w)

    def run(self):
        logger.info('Start Edge Bridge',
                    extra={'MESSAGE_ID': 'edge_bridge_start_bridge'})
        logger.info('Start data sync...',
                    extra={'MESSAGE_ID': 'edge_bridge__data_sync'})
        self.fill_api_clients_queue()
        self.filter_workers_pool.spawn(self.fill_resource_items_queue)
        spawn(self.queues_controller)
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
