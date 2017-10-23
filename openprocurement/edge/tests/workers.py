# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
import logging
from copy import deepcopy
from gevent import sleep, idle
from gevent.queue import Queue, Empty
from mock import MagicMock, patch, call
from munch import munchify
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound as RNF,
    ResourceGone
)
from openprocurement.edge.workers import ResourceItemWorker
from openprocurement.edge.storages.couchdb_plugin import CouchDBStorage
from openprocurement.edge.workers import logger
from openprocurement.edge.utils import TZ

logger.setLevel(logging.DEBUG)


class TestResourceItemWorker(unittest.TestCase):

    worker_config = {
        'resource': 'tenders',
        'client_inc_step_timeout': 0.1,
        'client_dec_step_timeout': 0.02,
        'drop_threshold_client_cookies': 1.5,
        'worker_sleep': 0.1,
        'retry_default_timeout': 0.5,
        'retries_count': 2,
        'queue_timeout': 0.3,
        'bulk_save_limit': 1,
        'bulk_save_interval': 0.1
    }

    def tearDown(self):
        self.worker_config['resource'] = 'tenders'
        self.worker_config['client_inc_step_timeout'] = 0.1
        self.worker_config['client_dec_step_timeout'] = 0.02
        self.worker_config['drop_threshold_client_cookies'] = 1.5
        self.worker_config['worker_sleep'] = 0.03
        self.worker_config['retry_default_timeout'] = 0.05
        self.worker_config['retries_count'] = 2
        self.worker_config['queue_timeout'] = 0.03

    def test_init(self):
        worker = ResourceItemWorker(
            'api_clients_queue', 'resource_items_queue', 'db',
            {'bulk_save_limit': 1, 'bulk_save_interval': 1},
            'retry_resource_items_queue')
        self.assertEqual(worker.api_clients_queue, 'api_clients_queue')
        self.assertEqual(worker.resource_items_queue, 'resource_items_queue')
        self.assertEqual(worker.db, 'db')
        self.assertEqual(worker.config,
                         {'bulk_save_limit': 1, 'bulk_save_interval': 1})
        self.assertEqual(worker.retry_resource_items_queue,
                         'retry_resource_items_queue')
        self.assertEqual(worker.exit, False)
        self.assertEqual(worker.update_doc, False)

    def test_add_to_retry_queue(self):
        retry_items_queue = Queue()
        worker = ResourceItemWorker(
            config_dict=self.worker_config,
            retry_resource_items_queue=retry_items_queue)
        retry_item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat(),
        }
        self.assertEqual(retry_items_queue.qsize(), 0)

        # Add to retry_resource_items_queue
        worker.add_to_retry_queue(retry_item)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(retry_items_queue.qsize(), 1)
        retry_item_from_queue = retry_items_queue.get()
        self.assertEqual(retry_item_from_queue['retries_count'], 1)
        self.assertEqual(retry_item_from_queue['timeout'],
                         worker.config['retry_default_timeout'] * 2)

        # Add to retry_resource_items_queue with status_code '429'
        worker.add_to_retry_queue(retry_item, status_code=429)
        retry_item_from_queue = retry_items_queue.get()
        self.assertEqual(retry_item_from_queue['retries_count'], 1)
        self.assertEqual(retry_item_from_queue['timeout'],
                         worker.config['retry_default_timeout'] * 2)

        # Drop from retry_resource_items_queue
        retry_item['retries_count'] = 3
        worker.add_to_retry_queue(retry_item)
        self.assertEqual(retry_items_queue.qsize(), 0)

        del worker

    def test__get_api_client_dict(self):
        api_clients_queue = Queue()
        client = MagicMock()
        client_dict = {
            'id': uuid.uuid4().hex,
            'client': client,
            'request_interval': 0
        }
        client_dict2 = {
            'id': uuid.uuid4().hex,
            'client': client,
            'request_interval': 0
        }
        api_clients_queue.put(client_dict)
        api_clients_queue.put(client_dict2)
        api_clients_info = {
            client_dict['id']: {
                'drop_cookies': False,
                'not_actual_count': 5,
                'request_interval': 3
            },
            client_dict2['id']: {
                'drop_cookies': True,
                'not_actual_count': 3,
                'request_interval': 2
            }
        }

        # Success test
        worker = ResourceItemWorker(
            api_clients_queue=api_clients_queue,
            config_dict=self.worker_config, api_clients_info=api_clients_info)
        self.assertEqual(worker.api_clients_queue.qsize(), 2)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, client_dict)

        # Get lazy client
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['not_actual_count'], 0)
        self.assertEqual(api_client['request_interval'], 0)

        # Empty queue test
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, None)

        # Exception when try renew cookies
        client.renew_cookies.side_effect = Exception('Can\'t renew cookies')
        worker.api_clients_queue.put(client_dict2)
        api_clients_info[client_dict2['id']]['drop_cookies'] = True
        api_client = worker._get_api_client_dict()
        self.assertIs(api_client, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(worker.api_clients_queue.get(), client_dict2)

        # Get api_client with raise Empty exception
        api_clients_queue.put(client_dict2)
        api_clients_queue.get = MagicMock(side_effect=Empty)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, None)
        del worker

    def test__get_resource_item_from_queue(self):
        items_queue = Queue()
        item = {'id': uuid.uuid4().hex,
                'dateModified': datetime.datetime.utcnow().isoformat()}
        items_queue.put(item)

        # Success test
        worker = ResourceItemWorker(resource_items_queue=items_queue,
                                    config_dict=self.worker_config)
        self.assertEqual(worker.resource_items_queue.qsize(), 1)
        resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, item)
        self.assertEqual(worker.resource_items_queue.qsize(), 0)

        # Empty queue test
        resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, None)
        del worker

    @patch('openprocurement_client.client.TendersClient')
    def test__get_resource_item_from_public(self, mock_api_client):
        item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat()
        }
        api_clients_queue = Queue()
        client_dict = {
            'id': uuid.uuid4().hex,
            'request_interval': 0.02,
            'client': mock_api_client
        }
        api_clients_queue.put(client_dict)
        api_clients_info =\
            {client_dict['id']: {'drop_cookies': False, 'request_durations': {}}}
        retry_queue = Queue()
        return_dict = {
            'data': {
                'id': item['id'],
                'dateModified': datetime.datetime.utcnow().isoformat()
            }
        }
        mock_api_client.get_resource_item.return_value = return_dict
        worker = ResourceItemWorker(api_clients_queue=api_clients_queue,
                                    config_dict=self.worker_config,
                                    retry_resource_items_queue=retry_queue,
                                    api_clients_info=api_clients_info)

        # Success test
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['request_interval'], 0.02)
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 0)
        self.assertEqual(public_item, return_dict['data'])

        # Not actual document form public
        item['dateModified'] = datetime.datetime.utcnow().isoformat()
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'], 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)

        # InvalidResponse
        mock_api_client.get_resource_item.side_effect =\
            InvalidResponse('invalid response')
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 2)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)

        # RequestFailed status_code=429
        mock_api_client.get_resource_item.side_effect = RequestFailed(
            munchify({'status_code': 429}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'], 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 3)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'],
                         worker.config['client_inc_step_timeout'])

        # RequestFailed status_code=429 with drop cookies
        api_client['request_interval'] = 2
        public_item = worker._get_resource_item_from_public(api_client, item)
        sleep(api_client['request_interval'])
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 4)

        # RequestFailed with status_code not equal 429
        mock_api_client.get_resource_item.side_effect = RequestFailed(
            munchify({'status_code': 404}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 5)

        # ResourceNotFound
        mock_api_client.get_resource_item.side_effect = RNF(
            munchify({'status_code': 404}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 6)

        # ResourceGone
        mock_api_client.get_resource_item.side_effect = ResourceGone(munchify(
            {'status_code': 410}
        ))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 6)

        # Exception
        api_client = worker._get_api_client_dict()
        mock_api_client.get_resource_item.side_effect =\
            Exception('text except')
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 7)

        del worker

    def test__add_to_bulk(self):
        retry_queue = Queue()
        old_date_modified = datetime.datetime.utcnow().isoformat()
        queue_resource_item = {
            'doc_type': 'Tender',
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat()
        }
        resource_item_doc_dict = {
            'doc_type': 'Tender',
            '_rev': '1-' + uuid.uuid4().hex,
            'id': queue_resource_item['id'],
            'dateModified': queue_resource_item['dateModified']
        }
        resource_item_dict = {
            'doc_type': 'Tender',
            'id': queue_resource_item['id'],
            'dateModified': queue_resource_item['dateModified']
        }
        worker = ResourceItemWorker(config_dict=self.worker_config,
                                    retry_resource_items_queue=retry_queue)
        worker.db = MagicMock()

        # Successfull adding to bulk
        start_length = len(worker.bulk)
        worker._add_to_bulk(resource_item_dict, queue_resource_item,
                            resource_item_doc_dict)
        end_length = len(worker.bulk)
        self.assertGreater(end_length, start_length)

        # Update exist doc in bulk
        start_length = len(worker.bulk)
        new_resource_item_dict = deepcopy(resource_item_dict)
        new_resource_item_dict['dateModified'] =\
            datetime.datetime.utcnow().isoformat()
        worker._add_to_bulk(new_resource_item_dict, queue_resource_item,
                            resource_item_doc_dict)
        end_length = len(worker.bulk)
        self.assertEqual(start_length, end_length)

        # Ignored dublicate in bulk
        start_length = end_length
        worker._add_to_bulk({
            'doc_type': 'Tender',
            'id': queue_resource_item['id'],
            '_id': queue_resource_item['id'],
            'dateModified': old_date_modified
        }, queue_resource_item, resource_item_dict)
        end_length = len(worker.bulk)
        self.assertEqual(start_length, end_length)
        del worker

    @patch('openprocurement.edge.workers.logger')
    @patch('openprocurement.edge.workers.datetime')
    def test_log_timeshift(self, mocked_datetime, mocked_logger):
        mocked_datetime.now.return_value = datetime.datetime(2017, 1, 1, 0, 1,
                                                             tzinfo=TZ)
        date_modified = datetime.datetime(2017, 1, 1, 0, 0,
                                          tzinfo=TZ).isoformat()
        worker = ResourceItemWorker(config_dict=self.worker_config)
        resource_item = {'id': uuid.uuid4().hex, 'dateModified': date_modified}
        worker.log_timeshift(resource_item)
        mocked_logger.debug.assert_called_once_with(
            'tender {} timeshift is 60.0 sec.'.format(resource_item['id']),
            extra={'DOCUMENT_TIMESHIFT': 60.0})


    def test__save_bulk_docs(self):
        self.worker_config['bulk_save_limit'] = 3
        retry_queue = Queue()
        worker = ResourceItemWorker(config_dict=self.worker_config,
                                    retry_resource_items_queue=retry_queue)
        doc_id_1 = uuid.uuid4().hex
        doc_id_2 = uuid.uuid4().hex
        doc_id_3 = uuid.uuid4().hex
        doc_id_4 = uuid.uuid4().hex
        date_modified = datetime.datetime.utcnow().isoformat()
        worker.bulk = {
            doc_id_1: {'id': doc_id_1, 'dateModified': date_modified},
            doc_id_2: {'id': doc_id_2, 'dateModified': date_modified},
            doc_id_3: {'id': doc_id_3, 'dateModified': date_modified},
            doc_id_4: {'id': doc_id_4, 'dateModified': date_modified}
        }
        update_return_value = [
            (True, doc_id_1, '1-' + uuid.uuid4().hex),
            (True, doc_id_2, '2-' + uuid.uuid4().hex),
            (False, doc_id_3, Exception(u'New doc with oldest dateModified.')),
            (False, doc_id_4, Exception(u'Document update conflict.'))
        ]
        worker.db = CouchDBStorage(
            {'storage': {'host': '127.0.0.1', 'port': 5984}}, 'tenders')
        worker.db.db.update = MagicMock(return_value=update_return_value)

        # Test success response from couchdb
        worker._save_bulk_docs()
        sleep(0.1)
        self.assertEqual(len(worker.bulk), 0)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)

        # Test failed response from couchdb
        worker.db.db.update.side_effect = Exception('Some exceptions')
        worker.bulk = {
            doc_id_1: {'id': doc_id_1, 'dateModified': date_modified},
            doc_id_2: {'id': doc_id_2, 'dateModified': date_modified},
            doc_id_3: {'id': doc_id_3, 'dateModified': date_modified},
            doc_id_4: {'id': doc_id_4, 'dateModified': date_modified}
        }
        worker._save_bulk_docs()
        sleep(0.2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 5)
        self.assertEqual(len(worker.bulk), 0)

    def test_shutdown(self):
        worker = ResourceItemWorker(
            'api_clients_queue', 'resource_items_queue', 'db',
            {'bulk_save_limit': 1, 'bulk_save_interval': 1},
            'retry_resource_items_queue')
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        self.assertEqual(worker.exit, True)

    def up_worker(self):
        worker_thread = ResourceItemWorker.spawn(
            resource_items_queue=self.queue,
            retry_resource_items_queue=self.retry_queue,
            api_clients_info=self.api_clients_info,
            api_clients_queue=self.api_clients_queue,
            config_dict=self.worker_config, db=self.db)
        idle()
        worker_thread.shutdown()
        sleep(3)

    @patch('openprocurement.edge.workers.ResourceItemWorker.'
           '_save_bulk_docs')
    @patch('openprocurement.edge.workers.ResourceItemWorker.'
           '_get_resource_item_from_public')
    @patch('openprocurement.edge.workers.logger')
    def test__run(self, mocked_logger, mock_get_from_public, mocked_save_bulk):
        self.queue = Queue()
        self.retry_queue = Queue()
        self.api_clients_queue = Queue()
        queue_item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat()
        }
        doc = {
            'id': queue_item['id'],
            '_rev': '1-{}'.format(uuid.uuid4().hex),
            'dateModified': datetime.datetime.utcnow().isoformat(),
            'doc_type': 'Tender'
        }
        client = MagicMock()
        api_client_dict = {
            'id': uuid.uuid4().hex,
            'client': client,
            'request_interval': 0
        }
        client.session.headers = {'User-Agent': 'Test-Agent'}
        self.api_clients_info = {
            api_client_dict['id']: {
                'drop_cookies': False, 'request_durations': []
            }
        }
        self.db = MagicMock()
        worker = ResourceItemWorker(
            api_clients_queue=self.api_clients_queue,
            resource_items_queue=self.queue,
            retry_resource_items_queue=self.retry_queue,
            db=self.db, api_clients_info=self.api_clients_info,
            config_dict=self.worker_config
        )
        worker.exit = MagicMock()
        worker.exit.__nonzero__.side_effect = [False, True]

        # Try get api client from clients queue
        self.assertEqual(self.queue.qsize(), 0)
        worker._run()
        self.assertEqual(self.queue.qsize(), 0)
        mocked_logger.debug.assert_called_once_with(
            'API clients queue is empty.')

        # Try get item from resource items queue
        self.api_clients_queue.put(api_client_dict)
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[1:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Resource items queue is empty.')
            ]
        )

        # Try get resource item from local storage
        self.queue.put(queue_item)
        mock_get_from_public.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[3:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], queue_item['dateModified'])),
                call('Put in bulk tender {} {}'.format(doc['id'],
                                                       doc['dateModified']))
            ]
        )

        # queue_resource_item dateModified is None and None public doc
        self.api_clients_queue.put(api_client_dict)
        self.queue.put({'id': doc['id'], 'dateModified': None})
        mock_get_from_public.return_value = None
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[6:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], None))
            ]
        )

        # queue_resource_item dateModified is None and not None public doc
        self.api_clients_queue.put(api_client_dict)
        self.api_clients_queue.put(api_client_dict)
        self.queue.put({'id': doc['id'], 'dateModified': None})
        mock_get_from_public.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[8:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], None)),
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Ignored dublicate tender {} in bulk: previous {}, '
                     'current {}'.format(
                    doc['id'], doc['dateModified'], doc['dateModified']),
                    extra={'MESSAGE_ID': 'skipped'})
            ]
        )

        # Add to retry queue
        self.api_clients_queue.put(api_client_dict)
        self.queue.put({'id': doc['id'], 'dateModified': None})
        mock_get_from_public.return_value = doc
        self.db.get_doc.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[12:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], None))
            ]
        )
        mocked_logger.info.assert_called_once_with(
            'Put tender {} to \'retries_queue\''.format(doc['id']),
            extra={'MESSAGE_ID': 'add_to_retry'}
        )

        # Skip doc
        self.api_clients_queue.put(api_client_dict)
        self.api_clients_queue.put(api_client_dict)
        self.queue.put({'id': doc['id'], 'dateModified': None})
        mock_get_from_public.return_value = doc
        self.db.get_doc.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[14:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], None)),
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Ignored tender {} QUEUE - {}, EDGE - {}'.format(
                    doc['id'], doc['dateModified'], doc['dateModified']),
                    extra={'MESSAGE_ID': 'skipped'})
            ]
        )
        self.assertEqual(mocked_logger.info.call_count, 1)

        # Skip doc with raise exception
        self.api_clients_queue.put(api_client_dict)
        self.api_clients_queue.put(api_client_dict)
        self.queue.put({'id': doc['id'], 'dateModified': None})
        mock_get_from_public.side_effect = Exception('test')
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[18:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], None))
            ]
        )
        mocked_logger.error.assert_called_once_with(
            'Error while getting resource item from couchdb: {}'.format(repr(
                Exception('test'))),
            extra={'MESSAGE_ID': 'exceptions'}
        )
        self.assertEqual(
            mocked_logger.info.call_args_list[1],
            call('Put tender {} to \'retries_queue\''.format(doc['id']),
                 extra={'MESSAGE_ID': 'add_to_retry'})
        )

        # Try get resource item from public server with None public doc
        new_date_modified = datetime.datetime.utcnow().isoformat()
        self.queue.put({'id': doc['id'], 'dateModified': new_date_modified})
        mock_get_from_public.return_value = None
        mock_get_from_public.side_effect = None
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[20:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], new_date_modified))
            ]
        )
        self.assertEqual(mocked_logger.info.call_count, 2)
        self.assertEqual(mocked_logger.error.call_count, 1)

        # Try get resource item from public server
        new_date_modified = datetime.datetime.utcnow().isoformat()
        self.queue.put({'id': doc['id'], 'dateModified': new_date_modified})
        mock_get_from_public.return_value = doc
        mock_get_from_public.side_effect = None
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[22:],
            [
                call('Got api_client ID: {} {}'.format(
                    api_client_dict['id'],
                    client.session.headers['User-Agent'])),
                call('Get tender {} {} from main queue.'.format(
                    doc['id'], new_date_modified)),
                call('Ignored dublicate tender {} in bulk: previous {}, '
                     'current {}'.format(
                    doc['id'], doc['dateModified'], doc['dateModified']),
                    extra={'MESSAGE_ID': 'skipped'})
            ]
        )
        self.assertEqual(mocked_logger.info.call_count, 2)
        self.assertEqual(mocked_logger.error.call_count, 1)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestResourceItemWorker))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
