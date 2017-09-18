# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import math
import logging
import logging.config
import os
import psutil
import argparse
import uuid
from couchdb import Server, Session
from httplib import IncompleteRead
from yaml import load
from urlparse import urlparse
from openprocurement_client.sync import ResourceFeeder
from openprocurement_client.exceptions import RequestFailed
from openprocurement_client.client import TendersClient as APIClient
from openprocurement.edge.utils import (
    prepare_couchdb,
    prepare_couchdb_views,
    DataBridgeConfigError
)
import gevent.pool
from gevent import spawn, sleep
from gevent.queue import Queue, Empty
from datetime import datetime, timedelta
from .workers import ResourceItemWorker

try:
    import urllib3.contrib.pyopenssl
    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

logger = logging.getLogger(__name__)

WORKER_CONFIG = {
    'resource': 'tenders',
    'client_inc_step_timeout': 0.1,
    'client_dec_step_timeout': 0.02,
    'drop_threshold_client_cookies': 2,
    'worker_sleep': 5,
    'retry_default_timeout': 3,
    'retries_count': 10,
    'queue_timeout': 3,
    'bulk_save_limit': 1000,
    'bulk_save_interval': 5
}

DEFAULTS = {
    'retrieve_mode': '_all_',
    'workers_inc_threshold': 75,
    'workers_dec_threshold': 35,
    'workers_min': 1,
    'workers_max': 3,
    'workers_pool': 3,
    'retry_workers_min': 1,
    'retry_workers_max': 2,
    'retry_workers_pool': 2,
    'retry_resource_items_queue_size': -1,
    'filter_workers_count': 1,
    'watch_interval': 10,
    'user_agent': 'edge.multi',
    'log_db_name': 'logs_db',
    'resource_items_queue_size': 10000,
    'input_queue_size': 10000,
    'resource_items_limit': 1000,
    'queues_controller_timeout': 60,
    'filter_workers_pool': 1,
    'bulk_query_interval': 5,
    'bulk_query_limit': 1000,
    'couch_url': 'http://127.0.0.1:5984',
    'db_name': 'edge_db',
    'perfomance_window': 300
}


class EdgeDataBridge(object):

    """Edge Bridge"""

    def __init__(self, config):
        super(EdgeDataBridge, self).__init__()
        self.config = config
        self.workers_config = {}
        self.bridge_id = uuid.uuid4().hex
        self.api_host = self.config_get('resources_api_server')
        self.api_version = self.config_get('resources_api_version')
        self.retrievers_params = self.config_get('retrievers_params')

        # Check up_wait_sleep
        up_wait_sleep = self.retrievers_params.get('up_wait_sleep')
        if up_wait_sleep is not None and up_wait_sleep < 30:
            raise DataBridgeConfigError('Invalid \'up_wait_sleep\' in '
                                        '\'retrievers_params\'. Value must be '
                                        'grater than 30.')

        # Workers settings
        for key in WORKER_CONFIG:
            self.workers_config[key] = (self.config_get(key) or
                                        WORKER_CONFIG[key])

        # Init config
        for key in DEFAULTS:
            setattr(self, key, self.config_get(key) or DEFAULTS[key])

        # Pools
        self.workers_pool = gevent.pool.Pool(self.workers_max)
        self.retry_workers_pool = gevent.pool.Pool(self.retry_workers_max)
        self.filter_workers_pool = gevent.pool.Pool(self.filter_workers_count)

        # Queues
        if self.input_queue_size == -1:
            self.input_queue = Queue()
        else:
            self.input_queue = Queue(self.input_queue_size)
        if self.resource_items_queue_size == -1:
            self.resource_items_queue = Queue()
        else:
            self.resource_items_queue = Queue(self.resource_items_queue_size)
        self.api_clients_queue = Queue()
        # self.retry_api_clients_queue = Queue()
        if self.retry_resource_items_queue_size == -1:
            self.retry_resource_items_queue = Queue()
        else:
            self.retry_resource_items_queue = Queue(
                self.retry_resource_items_queue_size)

        self.process = psutil.Process(os.getpid())

        if self.api_host != '' and self.api_host is not None:
            api_host = urlparse(self.api_host)
            if api_host.scheme == '' and api_host.netloc == '':
                raise DataBridgeConfigError(
                    'Invalid \'tenders_api_server\' url.')
        else:
            raise DataBridgeConfigError('In config dictionary empty or missing'
                                        ' \'tenders_api_server\'')
        self.db = prepare_couchdb(self.couch_url, self.db_name, logger)
        db_url = self.couch_url + '/' + self.db_name
        prepare_couchdb_views(db_url, self.workers_config['resource'], logger)
        self.server = Server(self.couch_url,
                             session=Session(retry_delays=range(10)))
        self.view_path = '_design/{}/_view/by_dateModified'.format(
            self.workers_config['resource'])
        extra_params = {
            'mode': self.retrieve_mode,
            'limit': self.resource_items_limit
        }
        self.feeder = ResourceFeeder(host=self.api_host,
                                     version=self.api_version, key='',
                                     resource=self.workers_config['resource'],
                                     extra_params=extra_params,
                                     retrievers_params=self.retrievers_params,
                                     adaptive=True)
        self.api_clients_info = {}

    def config_get(self, name):
        try:
            return self.config.get('main').get(name)
        except AttributeError:
            raise DataBridgeConfigError('In config dictionary missed section'
                                        ' \'main\'')

    def create_api_client(self):
        client_user_agent = self.user_agent + '/' + self.bridge_id
        timeout = 0.1
        while 1:
            try:
                api_client = APIClient(
                    host_url=self.api_host, user_agent=client_user_agent,
                    api_version=self.api_version, key='',
                    resource=self.workers_config['resource'])
                client_id = uuid.uuid4().hex
                logger.info('Started api_client {}'.format(
                    api_client.session.headers['User-Agent']),
                    extra={'MESSAGE_ID': 'create_api_clients'})
                api_client_dict = {
                    'id': client_id,
                    'client': api_client,
                    'request_interval': 0,
                    'not_actual_count': 0
                }
                self.api_clients_info[api_client_dict['id']] = {
                    'drop_cookies': False,
                    'request_durations': {},
                    'request_interval': 0,
                    'avg_duration': 0
                }
                self.api_clients_queue.put(api_client_dict)
                break
            except RequestFailed as e:
                logger.error(
                    'Failed start api_client with status code {}'.format(
                        e.status_code), extra={'MESSAGE_ID': 'exceptions'})
                timeout = timeout * 2
                logger.info(
                    'create_api_client will be sleep {} sec.'.format(timeout))
                sleep(timeout)
            except Exception as e:
                logger.error(
                    'Failed start api client with error: {}'.format(e.message),
                    extra={'MESSAGE_ID': 'exceptions'})
                timeout = timeout * 2
                logger.info(
                    'create_api_client will be sleep {} sec.'.format(timeout))
                sleep(timeout)

    def fill_api_clients_queue(self):
        while self.api_clients_queue.qsize() < self.workers_min:
            self.create_api_client()

    def fill_input_queue(self):
        for resource_item in self.feeder.get_resource_items():
            self.input_queue.put(resource_item)
            logger.debug('Add to temp queue from sync: {} {} {}'.format(
                self.workers_config['resource'][:-1], resource_item['id'],
                resource_item['dateModified']),
                extra={'MESSAGE_ID': 'received_from_sync'})

    def send_bulk(self, input_dict):
        sleep_before_retry = 2
        for i in xrange(0, 3):
            try:
                rows = self.db.view(self.view_path, keys=input_dict.values())
                resp_dict = {k.id: k.key for k in rows}
                break
            except (IncompleteRead, Exception) as e:
                logger.error('Error while send bulk {}'.format(e.message),
                             extra={'MESSAGE_ID': 'exceptions'})
                if i == 2:
                    raise e
                sleep(sleep_before_retry)
                sleep_before_retry *= 2
        for item_id, date_modified in input_dict.items():
            if item_id in resp_dict and date_modified == resp_dict[item_id]:
                logger.debug('Ignored {} {}: SYNC - {}, EDGE - {}'.format(
                    self.workers_config['resource'][:-1], item_id,
                    date_modified, resp_dict[item_id]),
                    extra={'MESSAGE_ID': 'skiped'})
            else:
                self.resource_items_queue.put(
                    {'id': item_id, 'dateModified': date_modified})
                logger.debug('Put to main queue {}: {} {}'.format(
                    self.workers_config['resource'][:-1], item_id,
                    date_modified),
                    extra={'MESSAGE_ID': 'add_to_resource_items_queue'})

    def fill_resource_items_queue(self):
        start_time = datetime.now()
        input_dict = {}
        while True:
            # Get resource_item from temp queue
            if not self.input_queue.empty():
                resource_item = self.input_queue.get()
            else:
                timeout = self.bulk_query_interval -\
                    (datetime.now() - start_time).total_seconds()
                if timeout > self.bulk_query_interval:
                    timeout = self.bulk_query_interval
                try:
                    resource_item = self.input_queue.get(timeout=timeout)
                except Empty:
                    resource_item = None

            # Add resource_item to bulk
            if resource_item is not None:
                input_dict[resource_item['id']] = resource_item['dateModified']

            if (len(input_dict) >= self.bulk_query_limit or
                (datetime.now() - start_time).total_seconds() >=
                    self.bulk_query_interval):
                if len(input_dict) > 0:
                    self.send_bulk(input_dict)
                    input_dict = {}
                start_time = datetime.now()

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
            logger.error(
                'Filter error: Error while getting {} {} from couchdb: '
                '{}'.format(self.workers_config['resource'][:-1], r_id,
                            e.message), extra={'MESSAGE_ID': 'exceptions'})
            return True

    def _get_average_requests_duration(self):
        req_durations = []
        delta = timedelta(seconds=self.perfomance_window)
        current_date = datetime.now() - delta
        for cid, info in self.api_clients_info.items():
            if len(info['request_durations']) > 0:
                if min(info['request_durations'].keys()) <= current_date:
                    info['grown'] = True
                avg = round(
                    sum(info['request_durations'].values()) * 1.0 /
                    len(info['request_durations']), 3)
                req_durations.append(avg)
                info['avg_duration'] = avg

        if len(req_durations) > 0:
            return round(sum(req_durations) /
                         len(req_durations), 3), req_durations
        else:
            return 0, req_durations

    # TODO: Add logic for restart sync if last response grater than some values
    # and no active tasks specific for resource

    def queues_controller(self):
        while True:
            if (self.workers_pool.free_count() > 0 and
                (self.resource_items_queue.qsize() >
                 ((float(self.resource_items_queue_size) / 100) *
                  self.workers_inc_threshold))):
                self.create_api_client()
                w = ResourceItemWorker.spawn(self.api_clients_queue,
                                             self.resource_items_queue,
                                             self.db, self.workers_config,
                                             self.retry_resource_items_queue,
                                             self.api_clients_info)
                self.workers_pool.add(w)
                logger.info('Queue controller: Create main queue worker.')
            elif (self.resource_items_queue.qsize() <
                  ((float(self.resource_items_queue_size) / 100) *
                   self.workers_dec_threshold)):
                if len(self.workers_pool) > self.workers_min:
                    wi = self.workers_pool.greenlets.pop()
                    wi.shutdown()
                    api_client_dict = self.api_clients_queue.get()
                    del self.api_clients_info[api_client_dict['id']]
                    logger.info('Queue controller: Kill main queue worker.')
            filled_resource_items_queue = round(
                self.resource_items_queue.qsize() /
                (float(self.resource_items_queue_size) / 100), 2)
            logger.info('Resource items queue filled on {} %'.format(
                filled_resource_items_queue))
            filled_retry_resource_items_queue \
                = round(self.retry_resource_items_queue.qsize() / float(
                    self.retry_resource_items_queue_size) / 100, 2)
            logger.info('Retry resource items queue filled on {} %'.format(
                filled_retry_resource_items_queue))
            sleep(self.queues_controller_timeout)

    def gevent_watcher(self):
        self.perfomance_watcher()
        for t in self.server.tasks():
            if (t['type'] == 'indexer' and t['database'] == self.db_name and
                    t.get('design_document', None) == '_design/{}'.format(
                        self.workers_config['resource'])):
                logger.info(
                    'Watcher: Waiting for end of view indexing. Current'
                    ' progress: {} %'.format(t['progress']))

        # Check fill threads
        input_threads = 1
        if self.input_queue_filler.exception:
            input_threads = 0
            logger.error('Temp queue filler error: {}'.format(
                self.input_queue_filler.exception.message),
                extra={'MESSAGE_ID': 'exception'})
            self.input_queue_filler = spawn(self.fill_input_queue)
        logger.info('Input threads {}'.format(input_threads),
                    extra={'INPUT_THREADS': input_threads})
        fill_threads = 1
        if self.filler.exception:
            fill_threads = 0
            logger.error('Fill thread error: {}'.format(
                self.filler.exception.message),
                extra={'MESSAGE_ID': 'exception'})
            self.filler = spawn(self.fill_resource_items_queue)
        logger.info('Filter threads {}'.format(fill_threads),
                    extra={'FILTER_THREADS': fill_threads})

        main_threads = self.workers_max - self.workers_pool.free_count()
        logger.info('Main threads {}'.format(main_threads),
                    extra={'MAIN_THREADS': main_threads})

        if len(self.workers_pool) < self.workers_min:
            for i in xrange(0, (self.workers_min - len(self.workers_pool))):
                w = ResourceItemWorker.spawn(self.api_clients_queue,
                                             self.resource_items_queue,
                                             self.db, self.workers_config,
                                             self.retry_resource_items_queue,
                                             self.api_clients_info)
                self.workers_pool.add(w)
                logger.info('Watcher: Create main queue worker.')
                self.create_api_client()
        retry_threads = self.retry_workers_max -\
            self.retry_workers_pool.free_count()
        logger.info('Retry threads {}'.format(retry_threads),
                    extra={'RETRY_THREADS': retry_threads})
        if len(self.retry_workers_pool) < self.retry_workers_min:
            for i in xrange(0, self.retry_workers_min -
                            len(self.retry_workers_pool)):
                self.create_api_client()
                w = ResourceItemWorker.spawn(self.api_clients_queue,
                                             self.retry_resource_items_queue,
                                             self.db, self.workers_config,
                                             self.retry_resource_items_queue,
                                             self.api_clients_info)
                self.retry_workers_pool.add(w)
                logger.info('Watcher: Create retry queue worker.')

        # Log queues size and API clients count
        main_queue_size = self.resource_items_queue.qsize()
        logger.info('Resource items queue size {}'.format(
            main_queue_size), extra={'MAIN_QUEUE_SIZE': main_queue_size})
        retry_queue_size = self.retry_resource_items_queue.qsize()
        logger.info('Resource items retry queue size {}'.format(
            retry_queue_size), extra={'RETRY_QUEUE_SIZE': retry_queue_size})
        api_clients_count = len(self.api_clients_info)
        logger.info('API Clients count: {}'.format(api_clients_count),
                    extra={'API_CLIENTS': api_clients_count})

    def _calculate_st_dev(self, values):
        if len(values) > 0:
            avg = sum(values) * 1.0 / len(values)
            variance = map(lambda x: (x - avg) ** 2, values)
            avg_variance = sum(variance) * 1.0 / len(variance)
            st_dev = math.sqrt(avg_variance)
            return round(st_dev, 3)
        else:
            return 0

    def _mark_bad_clients(self, dev):
        # Mark bad api clients
        for cid, info in self.api_clients_info.items():
            if info.get('grown', False) and info['avg_duration'] > dev:
                info['drop_cookies'] = True
                logger.debug(
                    'Perfomance watcher: Mark client {} as bad, avg.'
                    ' request_duration is {} sec.'.format(
                        cid, info['avg_duration']),
                    extra={'MESSAGE_ID': 'marked_as_bad'})
            elif info['avg_duration'] < dev and info['request_interval'] > 0:
                info['drop_cookies'] = True
                logger.debug(
                    'Perfomance watcher: Mark client {} as bad,'
                    ' request_interval is {} sec.'.format(
                        cid, info['request_interval']),
                    extra={'MESSAGE_ID': 'marked_as_bad'})

    def perfomance_watcher(self):
            avg_duration, values = self._get_average_requests_duration()
            for _, info in self.api_clients_info.items():
                delta = timedelta(
                    seconds=self.perfomance_window + self.watch_interval)
                current_date = datetime.now() - delta
                delete_list = []
                for key in info['request_durations']:
                    if key < current_date:
                        delete_list.append(key)
                for k in delete_list:
                    del info['request_durations'][k]
                delete_list = []

            st_dev = self._calculate_st_dev(values)
            if len(values) > 0:
                min_avg = min(values) * 1000
                max_avg = max(values) * 1000
            else:
                max_avg = 0
                min_avg = 0
            dev = round(st_dev + avg_duration, 3)

            logger.info(
                'Perfomance watcher:\nREQUESTS_STDEV - {} sec.\n'
                'REQUESTS_DEV - {} ms.\nREQUESTS_MIN_AVG - {} ms.\n'
                'REQUESTS_MAX_AVG - {} ms.\nREQUESTS_AVG - {} sec.'.format(
                    round(st_dev, 3), dev, min_avg, max_avg, avg_duration),
                extra={'REQUESTS_DEV': dev * 1000,
                       'REQUESTS_MIN_AVG': min_avg,
                       'REQUESTS_MAX_AVG': max_avg,
                       'REQUESTS_AVG': avg_duration * 1000})
            self._mark_bad_clients(dev)

    def run(self):
        logger.info('Start Edge Bridge',
                    extra={'MESSAGE_ID': 'edge_bridge_start_bridge'})
        logger.info('Start data sync...',
                    extra={'MESSAGE_ID': 'edge_bridge__data_sync'})
        self.input_queue_filler = spawn(self.fill_input_queue)
        self.filler = spawn(self.fill_resource_items_queue)
        spawn(self.queues_controller)
        while True:
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
