# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import gevent
from gevent import Greenlet
from gevent import spawn, sleep, idle
from gevent.queue import Queue, Empty
import logging
import logging.config
from openprocurement_client.exceptions import InvalidResponse, RequestFailed

logger = logging.getLogger(__name__)

class ResourceItemWorker(Greenlet):

    def __init__(self, api_clients_queue=None, resource_items_queue=None,
                 db=None, config_dict=None, retry_resource_items_queue=None,
                 log_dict=None):
        Greenlet.__init__(self)
        self.exit = False
        self.update_doc = False
        self.db = db
        self.config = config_dict
        self.log_dict = log_dict
        self.api_clients_queue = api_clients_queue
        self.resource_items_queue = resource_items_queue
        self.retry_resource_items_queue = retry_resource_items_queue

    def add_to_retry_queue(self, resource_item, status_code=0):
        timeout = resource_item.get('timeout') or self.config['retry_default_timeout']
        retries_count = resource_item.get('retries_count') or 0
        if status_code != 429:
            resource_item['timeout'] = timeout * 2
            resource_item['retries_count'] = retries_count + 1
        else:
            resource_item['timeout'] = timeout
            resource_item['retries_count'] = retries_count
        if resource_item['retries_count'] > self.config['retries_count']:
            self.log_dict['droped'] += 1
            logger.info('{} {} reached limit retries count {} and'
                        ' droped from retry_queue.'.format(
                            self.config['resource'][:-1].title(), resource_item['id'],
                            self.config['retries_count']))
        else:
            self.log_dict['add_to_retry'] += 1
            spawn(self.retry_resource_items_queue.put,
                  resource_item, timeout=timeout)
            logger.info('Put {} {} to \'retries_queue\''.format(
                self.config['resource'][:-1], resource_item['id']))

    def _get_api_client_dict(self):
        if not self.api_clients_queue.empty():
            api_client_dict = self.api_clients_queue.get(
                timeout=self.config['queue_timeout'])
            logger.info('Got api_client {}'.format(
                api_client_dict['client'].session.headers['User-Agent']
            ))
            return api_client_dict
        else:
            return None

    def _get_resource_item_from_queue(self):
        if not self.resource_items_queue.empty():
            queue_resource_item = self.resource_items_queue.get(
                timeout=self.config['queue_timeout'])
            return queue_resource_item
        else:
            return None

    def _get_resource_item_from_public(self, api_client_dict,
                                       queue_resource_item):
        try:
            logger.info('Request interval {} sec. for client {}'.format(
                api_client_dict['request_interval'],
                api_client_dict['client'].session.headers['User-Agent']))
            resource_item = api_client_dict['client'].get_resource_item(
                queue_resource_item['id']).get('data')  # Resource object from api server
            if api_client_dict['request_interval'] > 0:
                api_client_dict['request_interval'] -= self.config['client_dec_step_timeout']
            if resource_item['dateModified'] < queue_resource_item['dateModified']:
                self.log_dict['not_actual_docs_count'] += 1
                logger.info('Client {} got not actual {} document {}'
                            ' from public server.'.format(
                                api_client_dict['client'].session.headers['User-Agent'],
                                self.config['resource'][:-1],
                                queue_resource_item['id']))
                self.add_to_retry_queue({
                    'id': queue_resource_item['id'],
                    'dateModified': queue_resource_item['dateModified']
                })
                self.api_clients_queue.put(api_client_dict)
                return None
            self.api_clients_queue.put(api_client_dict)
            return resource_item
        except InvalidResponse as e:
            self.api_clients_queue.put(api_client_dict)
            logger.error('Error while getting resource item from api'
                         ' server {}: '.format(e.status_code))
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            self.log_dict['exceptions_count'] += 1
            return None
        except RequestFailed as e:
            if e.status_code == 429:
                if api_client_dict['request_interval'] > self.config['drop_threshold_client_cookies']:
                    api_client_dict['client'].session.cookies.clear()
                    api_client_dict['request_interval'] = 0
                else:
                    api_client_dict['request_interval'] += self.config['client_inc_step_timeout']
                spawn(self.api_clients_queue.put, api_client_dict,
                      timeout=api_client_dict['request_interval'])
            else:
                self.api_clients_queue.put(api_client_dict)
            logger.error('Request failed while getting resource item from'
                         ' api server with status code {}: '.format(
                             e.status_code))
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            }, status_code=e.status_code)
            self.log_dict['exceptions_count'] += 1
            return None
        except Exception as e:
            self.api_clients_queue.put(api_client_dict)
            logger.error('Error while getting resource item from api'
                         ' server {}: '.format(e.message))
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            self.log_dict['exceptions_count'] += 1
            return None

    def _save_to_db(self, resource_item, queue_resource_item,
                    resource_item_doc):
        resource_item['doc_type'] = self.config['resource'][:-1].title()
        resource_item['_id'] = resource_item['id']
        if resource_item_doc:
            resource_item['_rev'] = resource_item_doc['_rev']
            if resource_item['dateModified'] > resource_item_doc['dateModified']:
                logger.info('Update {} {} '.format(
                    self.config['resource'][:-1], queue_resource_item['id']))
                self.update_doc = True
            else:
                self.log_dict['skiped'] += 1
                return
        else:
            logger.info('Save {} {} '.format(
                self.config['resource'][:-1], queue_resource_item['id']))
            self.update_doc = False
        try:
            self.db.save(resource_item)
            if self.update_doc:
                self.log_dict['update_documents'] += 1
            else:
                self.log_dict['save_documents'] += 1
            return
        except Exception as e:
            logger.error('Saving {} {} fail with error {}'.format(
                self.config['resource'][:-1], queue_resource_item['id'], e.message),
                extra={'MESSAGE_ID': 'edge_bridge_fail_save_in_db'})
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']})
            self.log_dict['exceptions_count'] += 1
            return


    def _run(self):
        while not self.exit:
            # Try get api client from clients queue
            api_client_dict = self._get_api_client_dict()
            if api_client_dict is None:
                sleep(self.config['worker_sleep'])
                continue

            # Try get item from resource items queue
            queue_resource_item = self._get_resource_item_from_queue()
            if queue_resource_item is None:
                self.api_clients_queue.put(api_client_dict)
                sleep(self.config['worker_sleep'])
                continue

            # Try get resource item from local storage
            try:
                resource_item_doc = self.db.get(queue_resource_item['id'])  # Resource object from local db server
                if resource_item_doc and resource_item_doc['dateModified'] <= queue_resource_item['dateModified']:
                    self.api_clients_queue.put(api_client_dict)
                    self.log_dict['skiped'] += 1
                    continue
            except Exception as e:
                self.api_clients_queue.put(api_client_dict)
                self.add_to_retry_queue({
                    'id': queue_resource_item['id'],
                    'dateModified': queue_resource_item['dateModified']
                    })
                logger.error('Error while getting resource item from couchdb: {}'.format(
                    e.message
                ))
                self.log_dict['exceptions_count'] += 1
                continue

            # Try get resource item from public server
            resource_item = self._get_resource_item_from_public(
                api_client_dict, queue_resource_item)
            if resource_item is None:
                logger.info('{} {} not found'.format(
                    self.config['resource'][:-1].title(), queue_resource_item['id']))
                self.log_dict['not_found_count'] += 1
                continue

            # Save/Update resource item in db
            self._save_to_db(resource_item, queue_resource_item,
                             resource_item_doc)


    def shutdown(self):
        self.exit = True
        logger.info('Worker complete his job.')
