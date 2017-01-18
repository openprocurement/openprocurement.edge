# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
from couchdb import Server, ResourceNotFound
from gevent import sleep
from gevent.queue import Queue
from mock import MagicMock, patch
from munch import munchify
from openprocurement_client.client import TendersClient as APIClient
from openprocurement_client.exceptions import InvalidResponse, RequestFailed
from openprocurement.edge.workers import ResourceItemWorker
from socket import error


class TestResourceItemWorker(unittest.TestCase):

    worker_config = {
        'resource': 'tenders',
        'client_inc_step_timeout': 0.1,
        'client_dec_step_timeout': 0.02,
        'drop_threshold_client_cookies': 1.5,
        'worker_sleep': 3,
        'retry_default_timeout': 5,
        'retries_count': 2,
        'queue_timeout': 3
    }

    log_dict = {
        'not_actual_docs_count': 0,
        'update_documents': 0,
        'save_documents': 0,
        'add_to_retry': 0,
        'droped': 0,
        'skiped': 0,
        'add_to_resource_items_queue': 0,
        'exceptions_count': 0,
        'not_found_count': 0
    }

    def tearDown(self):
        self.worker_config['resource'] = 'tenders'
        self.worker_config['client_inc_step_timeout'] = 0.1
        self.worker_config['client_dec_step_timeout'] = 0.02
        self.worker_config['drop_threshold_client_cookies'] = 1.5
        self.worker_config['worker_sleep'] = 3
        self.worker_config['retry_default_timeout'] = 5
        self.worker_config['retries_count'] = 2
        self.worker_config['queue_timeout'] = 3

        self.log_dict['not_actual_docs_count'] = 0
        self.log_dict['update_documents'] = 0
        self.log_dict['save_documents'] = 0
        self.log_dict['add_to_retry'] = 0
        self.log_dict['droped'] = 0
        self.log_dict['skiped'] = 0
        self.log_dict['add_to_resource_items_queue'] = 0
        self.log_dict['exceptions_count'] = 0
        self.log_dict['not_found_count'] = 0


    def test_init(self):
        worker = ResourceItemWorker('api_clients_queue', 'resource_items_queue',
                                    'db', 'config_dict',
                                    'retry_resource_items_queue', 'log_dict')
        self.assertEqual(worker.api_clients_queue, 'api_clients_queue')
        self.assertEqual(worker.resource_items_queue, 'resource_items_queue')
        self.assertEqual(worker.db, 'db')
        self.assertEqual(worker.config, 'config_dict')
        self.assertEqual(worker.retry_resource_items_queue,
                         'retry_resource_items_queue')
        self.assertEqual(worker.log_dict, 'log_dict')
        self.assertEqual(worker.exit, False)
        self.assertEqual(worker.update_doc, False)

    def test_add_to_retry_queue(self):
        retry_items_queue = Queue()
        worker = ResourceItemWorker(config_dict=self.worker_config,
                                    retry_resource_items_queue=retry_items_queue,
                                    log_dict=self.log_dict)
        retry_item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat(),
        }
        self.assertEqual(retry_items_queue.qsize(), 0)
        self.assertEqual(worker.log_dict['add_to_retry'], 0)

        # Add to retry_resource_items_queue
        worker.add_to_retry_queue(retry_item)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(retry_items_queue.qsize(), 1)
        self.assertEqual(worker.log_dict['add_to_retry'], 1)
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
        self.assertEqual(worker.log_dict['droped'], 0)
        worker.add_to_retry_queue(retry_item)
        self.assertEqual(worker.log_dict['droped'], 1)
        self.assertEqual(retry_items_queue.qsize(), 0)

        del worker

    @patch('openprocurement.edge.tests.workers.APIClient')
    def test__get_api_client_dict(self, mock_api_client):
        api_clients_queue = Queue()
        client = mock_api_client()
        client_dict = {'client': client, 'request_interval': 0}
        api_clients_queue.put(client_dict)

        # Success test
        worker = ResourceItemWorker(api_clients_queue=api_clients_queue,
                                    config_dict=self.worker_config,
                                    log_dict=self.log_dict)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, client_dict)

        # Empty queue test
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
                                    config_dict=self.worker_config,
                                    log_dict=self.log_dict)
        self.assertEqual(worker.resource_items_queue.qsize(), 1)
        resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, item)
        self.assertEqual(worker.resource_items_queue.qsize(), 0)

        # Empty queue test
        resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, None)
        del worker

    @patch('openprocurement.edge.tests.workers.APIClient')
    def test__get_resource_item_from_public(self, mock_api_client):
        item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat()
        }
        api_clients_queue = Queue()
        api_clients_queue.put({
            'client': mock_api_client,
            'request_interval': 0.02})
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
                                    log_dict=self.log_dict)

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
        self.assertEqual(api_client['request_interval'], 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.log_dict['not_actual_docs_count'], 1)
        self.assertEqual(worker.log_dict['add_to_retry'], 1)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)

        # InvalidResponse
        mock_api_client.get_resource_item.side_effect = InvalidResponse('invalid response')
        self.assertEqual(self.log_dict['exceptions_count'], 0)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.log_dict['exceptions_count'], 1)
        self.assertEqual(worker.log_dict['add_to_retry'], 2)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 2)

        # RequestFailed status_code=429
        mock_api_client.get_resource_item.side_effect = RequestFailed(
            munchify({'status_code': 429}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['request_interval'], 0)
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.log_dict['exceptions_count'], 2)
        self.assertEqual(worker.log_dict['add_to_retry'], 3)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 3)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['request_interval'], worker.config['client_inc_step_timeout'])

        # RequestFailed status_code=429 with drop cookies
        api_client['request_interval'] = 2
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 3)
        self.assertEqual(worker.log_dict['add_to_retry'], 4)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 4)

        # RequestFailed with status_code not equal 429
        mock_api_client.get_resource_item.side_effect = RequestFailed(
            munchify({'status_code': 404}))
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 4)
        self.assertEqual(worker.log_dict['add_to_retry'], 5)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 5)

        # Exception
        api_client = worker._get_api_client_dict()
        mock_api_client.get_resource_item.side_effect = Exception('text except')
        public_item = worker._get_resource_item_from_public(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 5)
        self.assertEqual(worker.log_dict['add_to_retry'], 6)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 6)

        del worker


    def test__save_to_db(self):
        retry_queue = Queue()
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
                                    log_dict=self.log_dict,
                                    retry_resource_items_queue=retry_queue)
        worker.db = MagicMock()
        # Skip test
        self.assertEqual(worker.log_dict['skiped'], 0)
        self.assertEqual(worker.log_dict['update_documents'], 0)
        self.assertEqual(worker.log_dict['save_documents'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 0)
        worker._save_to_db(resource_item_dict, queue_resource_item,
                           resource_item_doc_dict)
        self.assertEqual(worker.log_dict['skiped'], 1)
        self.assertEqual(worker.log_dict['update_documents'], 0)
        self.assertEqual(worker.log_dict['save_documents'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 0)

        # Update test
        resource_item_dict['dateModified'] = datetime.datetime.utcnow().isoformat()
        worker._save_to_db(resource_item_dict, queue_resource_item,
                           resource_item_doc_dict)
        self.assertEqual(worker.log_dict['update_documents'], 1)
        self.assertEqual(worker.log_dict['save_documents'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 0)

        # Save test
        worker._save_to_db(resource_item_dict, queue_resource_item,
                           None)
        self.assertEqual(worker.log_dict['save_documents'], 1)
        self.assertEqual(worker.log_dict['exceptions_count'], 0)

        # Exception test
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 0)
        worker.db.save.side_effect = Exception('Save Exception')
        worker._save_to_db(resource_item_dict, queue_resource_item,
                           None)
        self.assertEqual(worker.log_dict['exceptions_count'], 1)
        sleep(worker.config['retry_default_timeout'])
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)

    def test_shutdown(self):
        worker = ResourceItemWorker('api_clients_queue', 'resource_items_queue',
                                    'db', 'config_dict',
                                    'retry_resource_items_queue', 'log_dict')
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        self.assertEqual(worker.exit, True)
