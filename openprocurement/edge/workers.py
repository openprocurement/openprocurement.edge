# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

from datetime import datetime
from gevent import Greenlet
from gevent import spawn, sleep
import logging
import logging.config
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound
)

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
        self.bulk = {}
        self.bulk_save_limit = self.config['bulk_save_limit']
        self.bulk_save_interval = self.config['bulk_save_interval']
        self.start_time = datetime.now()

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
            logger.critical('{} {} reached limit retries count {} and'
                            ' droped from retry_queue.'.format(
                                self.config['resource'][:-1].title(),
                                resource_item['id'],
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
            resource_item = api_client_dict['client'].get_resource_item(
                queue_resource_item['id']).get('data')  # Resource object from api server
            logger.debug('Recieved from API {}: {} {}'.format(
                self.config['resource'][:-1], resource_item['id'],
                resource_item['dateModified']))
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
                return None  # Not actual
            self.api_clients_queue.put(api_client_dict)
            return resource_item
        except InvalidResponse as e:
            self.api_clients_queue.put(api_client_dict)
            logger.error('Error while getting {} {} from public with '
                         'status code: {}'.format(
                             self.config['resource'][:-1],
                             queue_resource_item['id'],
                             e.status_code))
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
            logger.error('Request failed while getting {} {} from public'
                         ' with status code {}: '.format(
                             self.config['resource'][:-1],
                             queue_resource_item['id'], e.status_code))
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            }, status_code=e.status_code)
            self.log_dict['exceptions_count'] += 1
            return None  # request failed
        except ResourceNotFound as e:
            logger.error('Resource not found {} at public: {} {}. {}'.format(
                self.config['resource'][:-1], queue_resource_item['id'],
                queue_resource_item['dateModified'], e.message))
            api_client_dict['client'].session.cookies.clear()
            logger.info('Clear client cookies')
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            self.log_dict['not_found_count'] += 1
            self.log_dict['exceptions_count'] += 1
            self.api_clients_queue.put(api_client_dict)
            return None  # not found
        except Exception as e:
            self.api_clients_queue.put(api_client_dict)
            logger.error('Error while getting resource item {} {} {} from'
                         ' public {}: '.format(
                             self.config['resource'][:-1],
                             queue_resource_item['id'],
                             queue_resource_item['dateModified'], e.message))
            self.add_to_retry_queue({
                'id': queue_resource_item['id'],
                'dateModified': queue_resource_item['dateModified']
            })
            self.log_dict['exceptions_count'] += 1
            return None

    def _add_to_bulk(self, resource_item, queue_resource_item,
                     resource_item_doc):
        resource_item['doc_type'] = self.config['resource'][:-1].title()
        resource_item['_id'] = resource_item['id']
        if resource_item_doc:
            resource_item['_rev'] = resource_item_doc['_rev']
        bulk_doc = self.bulk.get(resource_item['id'])

        if bulk_doc and bulk_doc['dateModified'] < resource_item['dateModified']:
            logger.debug('Replaced {} in bulk {} previous {}, current {}'.format(
                self.config['resource'][:-1], bulk_doc['id'],
                bulk_doc['dateModified'], resource_item['dateModified']))
            self.bulk[resource_item['id']] = resource_item
        elif bulk_doc and bulk_doc['dateModified'] >= resource_item['dateModified']:
            logger.debug('Ignored dublicate {} {} in bulk: previous {},'
                         ' current {}'.format(
                             self.config['resource'][:-1], resource_item['id'],
                             bulk_doc['dateModified'],
                             resource_item['dateModified']))
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
            try:
                res = self.db.update(self.bulk.values())
                logger.info('Save bulk docs to db.')
            except Exception as e:
                logger.error('Error while saving bulk_docs in db: {}'.format(
                    e.message
                ))
                for doc in self.bulk.values():
                    self.add_to_retry_queue({'id': doc['id'],
                                             'dateModified': doc['dateModified']})
                self.bulk = {}
                self.start_time = datetime.now()
                return
            self.bulk = {}
            for success, doc_id, rev_or_exc in res:
                if success:
                    if not rev_or_exc.startswith('1-'):
                        self.log_dict['update_documents'] += 1
                        logger.info('Update {} {}'.format(
                            self.config['resource'][:-1], doc_id))
                    else:
                        self.log_dict['save_documents'] += 1
                        logger.info('Save {} {}'.format(
                            self.config['resource'][:-1], doc_id))
                    continue
                else:
                    if rev_or_exc.message != u'New doc with oldest dateModified.':
                        self.add_to_retry_queue({'id': doc_id,
                                                 'dateModified': None})
                        logger.error('Put to retry queue {} {} with '
                                     'reason: {}'.format(
                                         self.config['resource'][:-1],
                                         doc_id, rev_or_exc.message))
                    else:
                        logger.debug('Ignored {} {} with reason: {}'.format(
                            self.config['resource'][:-1], doc_id,
                            rev_or_exc))
                        self.log_dict['skiped'] += 1
                        continue
            self.start_time = datetime.now()

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
                if queue_resource_item['dateModified'] is None:
                    public_doc = self._get_resource_item_from_public(
                        api_client_dict, queue_resource_item)
                    if public_doc:
                        queue_resource_item['dateModified'] \
                            = public_doc['dateModified']
                    else:
                        continue
                    api_client_dict = self._get_api_client_dict()
                    if api_client_dict is None:
                        self.add_to_retry_queue(queue_resource_item)
                        continue
                if resource_item_doc and resource_item_doc['dateModified'] >= queue_resource_item['dateModified']:
                    self.log_dict['skiped'] += 1
                    logger.debug('Ignored {} {} QUEUE - {}, EDGE - {}'.format(
                        self.config['resource'][:-1],
                        queue_resource_item['id'],
                        queue_resource_item['dateModified'],
                        resource_item_doc['dateModified'],
                    ))
                    self.api_clients_queue.put(api_client_dict)
                    continue
            except Exception as e:
                self.api_clients_queue.put(api_client_dict)
                self.add_to_retry_queue({
                    'id': queue_resource_item['id'],
                    'dateModified': queue_resource_item['dateModified']
                })
                logger.error('Error while getting resource item from couchdb: '
                             '{}'.format(e.message))
                self.log_dict['exceptions_count'] += 1
                continue

            # Try get resource item from public server
            resource_item = self._get_resource_item_from_public(
                api_client_dict, queue_resource_item)
            if resource_item is None:
                continue

            # Add docs to bulk
            self._add_to_bulk(resource_item, queue_resource_item,
                              resource_item_doc)

            # Save/Update docs in db
            self._save_bulk_docs()

    def shutdown(self):
        self.exit = True
        logger.info('Worker complete his job.')
