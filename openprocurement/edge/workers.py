# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import os
from datetime import datetime
from gevent import Greenlet
from gevent import spawn, sleep
from gevent.queue import Empty
from pytz import timezone
from requests.exceptions import ConnectionError
import logging
import logging.config
import time
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound
)
from openprocurement.edge.utils import StorageException

logger = logging.getLogger(__name__)

TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


class ResourceItemWorker(Greenlet):

    def __init__(self, api_clients_queue=None, resource_items_queue=None,
                 storage=None, config_dict=None,
                 retry_resource_items_queue=None, api_clients_info=None):
        Greenlet.__init__(self)
        self.exit = False
        self.storage = storage
        self.config = config_dict
        self.api_clients_queue = api_clients_queue
        self.resource_items_queue = resource_items_queue
        self.retry_resource_items_queue = retry_resource_items_queue
        self.api_clients_info = api_clients_info
        self.bulk = {}
        self.bulk_save_limit = self.config['bulk_save_limit']
        self.bulk_save_interval = self.config['bulk_save_interval']
        self.start_time = datetime.now()

    def add_to_retry_queue(self, resource_item, status_code=0):
        timeout = resource_item.get('timeout') or\
            self.config['retry_default_timeout']
        retries_count = resource_item.get('retries_count') or 0
        if status_code != 429:
            resource_item['timeout'] = timeout * 2
            resource_item['retries_count'] = retries_count + 1
        else:
            resource_item['timeout'] = timeout
            resource_item['retries_count'] = retries_count
        if resource_item['retries_count'] > self.config['retries_count']:
            logger.critical(
                '{} {} reached limit retries count {} and droped from '
                'retry_queue.'.format(
                    self.config['resource'][:-1].title(),
                    resource_item['id'], self.config['retries_count']),
                extra={'MESSAGE_ID': 'droped_documents'})
        else:
            spawn(self.retry_resource_items_queue.put,
                  resource_item, timeout=timeout)
            logger.info('Put {} {} to \'retries_queue\''.format(
                self.config['resource'][:-1], resource_item['id']),
                extra={'MESSAGE_ID': 'add_to_retry'})

    def _add_to_bulk(self, resource_item, queue_resource_item):
        resource_item['doc_type'] = self.config['resource'][:-1].title()
        resource_item['_id'] = resource_item['id']
        _rev = getattr(self.storage, 'checked_doc_rev', None)
        if _rev is not None:
            resource_item['_rev'] = _rev
        bulk_doc = self.bulk.get(resource_item['id'])

        if bulk_doc and bulk_doc['dateModified'] <\
                resource_item['dateModified']:
            logger.debug(
                'Replaced {} in bulk {} previous {}, current {}'.format(
                    self.config['resource'][:-1], bulk_doc['id'],
                    bulk_doc['dateModified'], resource_item['dateModified']),
                extra={'MESSAGE_ID': 'skiped'})
            self.bulk[resource_item['id']] = resource_item
        elif bulk_doc and bulk_doc['dateModified'] >=\
                resource_item['dateModified']:
            logger.debug(
                'Ignored dublicate {} {} in bulk: previous {}, current '
                '{}'.format(
                    self.config['resource'][:-1], resource_item['id'],
                    bulk_doc['dateModified'], resource_item['dateModified']),
                extra={'MESSAGE_ID': 'skiped'})
        if not bulk_doc:
            self.bulk[resource_item['id']] = resource_item
            logger.debug('Put in bulk {} {} {}'.format(
                self.config['resource'][:-1], resource_item['id'],
                resource_item['dateModified']))
        return

    def _save_bulk_docs(self):
        if (len(self.bulk) > self.bulk_save_limit or
                (datetime.now() - self.start_time).total_seconds() >
                self.bulk_save_interval or self.exit):

            add_to_retry = self.storage.save(self.bulk.values())
            self.bulk = {}
            if len(add_to_retry) > 0:
                for i in add_to_retry:
                    self.add_to_retry_queue(i)
            self.start_time = datetime.now()

    def _get_api_client_dict(self):
        if not self.api_clients_queue.empty():
            try:
                api_client_dict = self.api_clients_queue.get(
                    timeout=self.config['queue_timeout'])
            except Empty:
                return None
            if self.api_clients_info[api_client_dict['id']]['drop_cookies']:
                try:
                    api_client_dict['client'].renew_cookies()
                    self.api_clients_info[api_client_dict['id']] = {
                        'drop_cookies': False,
                        'request_durations': {},
                        'request_interval': 0,
                        'avg_duration': 0
                    }
                    api_client_dict['request_interval'] = 0
                    api_client_dict['not_actual_count'] = 0
                    logger.info('Drop lazy api_client {} cookies'.format(
                        api_client_dict['id']))
                except (Exception, ConnectionError) as e:
                    logger.error('While renewing cookies catch exception: '
                                 '{}'.format(e.message))
                    return None

            logger.debug('Got api_client ID: {} {}'.format(
                api_client_dict['id'],
                api_client_dict['client'].session.headers['User-Agent']))
            return api_client_dict
        else:
            return None

    def _get_resource_item_from_queue(self):
        if not self.resource_items_queue.empty():
            queue_resource_item = self.resource_items_queue.get(
                timeout=self.config['queue_timeout'])
            logger.debug('Get {} {} {} from main queue.'.format(
                self.config['resource'][:-1], queue_resource_item['id'],
                queue_resource_item['dateModified']))
            return queue_resource_item
        else:
            return None

    def _get_resource_item_from_public(self, api_client_dict,
                                       queue_resource_item):
        try:
            logger.info('Request interval {} sec. for client {}'.format(
                api_client_dict['request_interval'],
                api_client_dict['client'].session.headers['User-Agent']))
            start = time.time()
            resource_item = api_client_dict['client'].get_resource_item(
                queue_resource_item['id']).get('data')
            self.api_clients_info[api_client_dict['id']][
                'request_durations'][datetime.now()] = time.time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] =\
                api_client_dict['request_interval']
            logger.debug('Recieved from API {}: {} {}'.format(
                self.config['resource'][:-1], resource_item['id'],
                resource_item['dateModified']))
            if api_client_dict['request_interval'] > 0:
                api_client_dict['request_interval'] -=\
                    self.config['client_dec_step_timeout']
            if resource_item['dateModified'] <\
                    queue_resource_item['dateModified']:
                logger.info(
                    'Client {} got not actual {} document {} from public '
                    'server.'.format(
                        api_client_dict['client'].session.headers[
                            'User-Agent'], self.config['resource'][:-1],
                        queue_resource_item['id']),
                    extra={'MESSAGE_ID': 'not_actual_docs'})
                self.add_to_retry_queue({
                    'id': queue_resource_item['id'],
                    'dateModified': queue_resource_item['dateModified']
                })
                self.api_clients_queue.put(api_client_dict)
                return None  # Not actual
            self.api_clients_queue.put(api_client_dict)
            return resource_item
        except InvalidResponse as e:
            self.api_clients_info[api_client_dict['id']][
                'request_durations'][datetime.now()] = time.time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] =\
                api_client_dict['request_interval']
            self.api_clients_queue.put(api_client_dict)
            logger.error(
                'Error while getting {} {} from public with status code: '
                '{}'.format(
                    self.config['resource'][:-1], queue_resource_item['id'],
                    e.status_code), extra={'MESSAGE_ID': 'exceptions'})
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            return None
        except RequestFailed as e:
            self.api_clients_info[api_client_dict['id']][
                'request_durations'][datetime.now()] = time.time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] =\
                api_client_dict['request_interval']
            if e.status_code == 429:
                if (api_client_dict['request_interval'] >
                        self.config['drop_threshold_client_cookies']):
                    api_client_dict['client'].session.cookies.clear()
                    api_client_dict['request_interval'] = 0
                else:
                    api_client_dict['request_interval'] +=\
                        self.config['client_inc_step_timeout']
                spawn(self.api_clients_queue.put, api_client_dict,
                      timeout=api_client_dict['request_interval'])
            else:
                self.api_clients_queue.put(api_client_dict)
            logger.error(
                'Request failed while getting {} {} from public with status '
                'code {}: '.format(
                    self.config['resource'][:-1], queue_resource_item['id'],
                    e.status_code), extra={'MESSAGE_ID': 'exceptions'})
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            }, status_code=e.status_code)
            return None  # request failed
        except ResourceNotFound as e:
            self.api_clients_info[api_client_dict['id']][
                'request_durations'][datetime.now()] = time.time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] =\
                api_client_dict['request_interval']
            logger.error('Resource not found {} at public: {} {}. {}'.format(
                self.config['resource'][:-1], queue_resource_item['id'],
                queue_resource_item['dateModified'], e.message),
                extra={'MESSAGE_ID': 'not_found_docs'})
            api_client_dict['client'].session.cookies.clear()
            logger.info('Clear client cookies')
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            self.api_clients_queue.put(api_client_dict)
            return None  # not found
        except Exception as e:
            self.api_clients_info[api_client_dict['id']][
                'request_durations'][datetime.now()] = time.time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] =\
                api_client_dict['request_interval']
            self.api_clients_queue.put(api_client_dict)
            logger.error(
                'Error while getting resource item {} {} {} from public '
                '{}: '.format(
                    self.config['resource'][:-1], queue_resource_item['id'],
                    queue_resource_item['dateModified'], e.message),
                extra={'MESSAGE_ID': 'exceptions'})
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            return None

    def _run(self):
        while not self.exit:
            if len(self.bulk) > 0:
                self._save_bulk_docs()
            # Try get api client from clients queue
            api_client_dict = self._get_api_client_dict()
            if api_client_dict is None:
                logger.debug('API clients queue is empty.')
                sleep(self.config['worker_sleep'])
                continue  # pragma: no cover

            # Try get item from resource items queue
            queue_resource_item = self._get_resource_item_from_queue()
            if queue_resource_item is None:
                self.api_clients_queue.put(api_client_dict)
                logger.debug('Resource items queue is empty.')
                sleep(self.config['worker_sleep'])
                continue  # pragma: no cover

            # If dateModified is None set from public
            if queue_resource_item['dateModified'] is None:
                public_doc = self._get_resource_item_from_public(
                    api_client_dict, queue_resource_item)
                if public_doc:
                    queue_resource_item['dateModified'] \
                        = public_doc['dateModified']
                else:
                    continue  # pragma: no cover
                api_client_dict = self._get_api_client_dict()
                if api_client_dict is None:
                    self.add_to_retry_queue(queue_resource_item)
                    continue  # pragma: no cover

            # Check in storage db
            try:
                actual_in_storage = self.storage.check(queue_resource_item)
            except StorageException:
                self.add_to_retry_queue(queue_resource_item)
                self.api_clients_queue.put(api_client_dict)
                continue  # pragma: no cover
            if actual_in_storage:
                logger.debug('Ignored {} {} QUEUE - {}'.format(
                    self.config['resource'][:-1],
                    queue_resource_item['id'],
                    queue_resource_item['dateModified']),
                    extra={'MESSAGE_ID': 'skiped'})
                self.api_clients_queue.put(api_client_dict)
                continue  # pragma: no cover

            # Try get resource item from public server
            resource_item = self._get_resource_item_from_public(
                api_client_dict, queue_resource_item)
            if resource_item is None:
                continue  # pragma: no cover

            # Add docs to bulk
            self._add_to_bulk(resource_item, queue_resource_item)

            # Save/Update docs in db if exit == True
            if self.exit:
                add_to_retry = self.storage.save(self.bulk.values())
                if len(add_to_retry) > 0:
                    for r in add_to_retry:
                        self.add_to_retry_queue(r)
                    self.start_time = datetime.now()
                    self.bulk = {}

    def shutdown(self):
        self.exit = True
        logger.info('Worker complete his job.')
