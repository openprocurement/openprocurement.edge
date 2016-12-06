# -*- coding: utf-8 -*-
import unittest

from openprocurement.edge.tests.base import test_tender_data, TenderBaseWebTest
import uuid
from openprocurement.edge.databridge import EdgeDataBridge, DataBridgeConfigError
from mock import MagicMock, patch

import datetime
import io
import logging
from requests.exceptions import ConnectionError
from couchdb import Server, Database, Session
from gevent import sleep
from socket import error

logger = logging.getLogger()
logger.level = logging.DEBUG


class TestEdgeDataBridge(TenderBaseWebTest):
    config = {
        'main': {
            'tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'tenders_api_version': "0",
            'public_tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'couch_url': 'http://localhost:5984',
            'public_db': 'test_db',
            'queue_size': 101,
            'api_clients_count': 3,
            'workers_count': 3,
            'retry_workers_count': 2,
            'filter_workers_count': 1,
            'retry_workers_count': 2,
            'retry_default_timeout': 5,
            'worker_sleep': 5,
            'watch_interval': 10,
            'queue_timeout': 5,
            'retrievers_params': {
                'down_requests_sleep': 5,
                'up_requests_sleep': 1,
                'up_wait_sleep': 30,
                'queue_size': 101
            }
        },
        'version': 1
    }


    def test_init(self):
        bridge = EdgeDataBridge(self.config)
        self.assertIn('tenders_api_server', bridge.config['main'])
        self.assertIn('tenders_api_version', bridge.config['main'])
        self.assertIn('public_tenders_api_server', bridge.config['main'])
        self.assertIn('couch_url', bridge.config['main'])
        self.assertIn('public_db', bridge.config['main'])
        self.assertEqual(self.config['main']['couch_url'], bridge.couch_url)

        test_config = {}

        # Create EdgeDataBridge object with wrong config variable structure
        test_config = {
           'mani': {
                'tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'tenders_api_version': "0",
                'public_tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'couch_url': 'http://localhost:5984',
                'public_db': 'test_db',
            },
            'version': 1
        }
        with self.assertRaises(DataBridgeConfigError):
             EdgeDataBridge(test_config)

        # Create EdgeDataBridge object without variable 'tenders_api_server' in config
        del test_config['mani']
        test_config['main'] = {}
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)
        with self.assertRaises(KeyError):
            test_config['main']['tenders_api_server']

        # Create EdgeDataBridge object with empty tenders_api_server
        test_config['main']['tenders_api_server'] = ''
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with invalid tenders_api_server
        test_config['main']['tenders_api_server'] = 'my_server'
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        test_config['main']['tenders_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        test_config['main']['public_db'] = 'public'
        test_config['main']['tenders_api_version'] = "0"
        test_config['main']['public_tenders_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        # Create EdgeDataBridge object with non exist database
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(bridge.db.name, test_config['main']['public_db'])
        server = Server(test_config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[test_config['main']['public_db']]


        with patch('openprocurement.edge.databridge.Server.create') as mock_create:
            mock_create.side_effect = error('test error')
            with self.assertRaises(DataBridgeConfigError) as e:
                bridge = EdgeDataBridge(test_config)
                import pdb; pdb.set_trace()

        # Create EdgeDataBridge object with deleting config variables step by step
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError):
            test_config['main']['couch_url']
        del bridge

        del test_config['main']['tenders_api_version']
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError):
            test_config['main']['tenders_api_version']
        del bridge

        del test_config['main']['public_tenders_api_server']
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError):
            test_config['main']['public_tenders_api_server']
        del bridge
        server = Server(test_config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[test_config['main']['public_db']]

    def test_get_db_activity(self):
        # Success test
        bridge = EdgeDataBridge(self.config)
        db_info = bridge.get_db_activity()
        self.assertEqual(db_info['update_seq'], 0)
        self.assertEqual(db_info['doc_count'], 0)

        # Unsuccess test
        bridge.db = Database('http://localhost:5984/temp_db',
                             session=Session(retry_delays=range(10)))
        db_info = bridge.get_db_activity()
        self.assertEqual(db_info['update_seq'], -1)
        self.assertEqual(db_info['doc_count'], -1)
        del bridge
        server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[self.config['main']['public_db']]

    def test_add_to_retry_queue(self):
        bridge = EdgeDataBridge(self.config)
        retry_item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat(),
            'timeout': 1,
            'retries_count': 2
        }
        self.assertEqual(bridge.retry_resource_items_queue.qsize(), 0)
        bridge.add_to_retry_queue(retry_item)
        sleep(retry_item['timeout'] * 2)
        self.assertEqual(bridge.retry_resource_items_queue.qsize(), 1)
        retry_item_from_queue = bridge.retry_resource_items_queue.get()
        retry_item['timeout'] = retry_item['timeout'] * 2
        retry_item['retries_count'] += 1
        self.assertEqual(retry_item, retry_item_from_queue)
        retry_item['retries_count'] = 10
        bridge.add_to_retry_queue(retry_item)
        self.assertEqual(bridge.retry_resource_items_queue.qsize(), 0)
        del bridge
        server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[self.config['main']['public_db']]

    def test_fill_api_clients_queue(self):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.api_clients_queue.qsize(), 0)
        bridge.fill_api_clients_queue()
        self.assertEqual(bridge.api_clients_queue.qsize(), bridge.api_clients_count)
        server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[self.config['main']['public_db']]

    @patch('openprocurement.edge.databridge.get_resource_items')
    def test_fill_resource_items_queue(self, mock_get_resource_items):
        bridge = EdgeDataBridge(self.config)
        mock_get_resource_items.return_value = [{
            'id':uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat()}]
        self.assertEqual(bridge.resource_items_queue.qsize(), 0)
        bridge.fill_resource_items_queue()
        self.assertEqual(bridge.resource_items_queue.qsize(), 1)
        server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[self.config['main']['public_db']]

    def test_gevent_watcher(self):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.log_dict, {})
        bridge.gevent_watcher()
        log_dict = {
            'resource_items_queue_size': 0,
            'retry_resource_items_queue_size': 0,
            'workers_count': 0,
            'filter_workers_count': 0,
            'retry_workers_count': 0,
            'free_api_clients': 0,
            'write_documents': 0,
            'update_documents': 0
        }
        self.assertEqual(bridge.log_dict, log_dict)
        del bridge
        server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[self.config['main']['public_db']]

    def test_config_get(self):
        test_config = {
            'main': {
                'tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'tenders_api_version': "0",
                'public_tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'couch_url': 'http://localhost:5984',
                'public_db': 'test_db'
            },
            'version': 1
        }

        bridge = EdgeDataBridge(test_config)
        couch_url_config = bridge.config_get('couch_url')
        self.assertEqual(couch_url_config, test_config['main']['couch_url'])

        del bridge.config['main']['couch_url']
        couch_url_config = bridge.config_get('couch_url')
        self.assertEqual(couch_url_config, None)
        server = Server(test_config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[test_config['main']['public_db']]

        del bridge.config['main']
        with self.assertRaises(DataBridgeConfigError):
            bridge.config_get('couch_url')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestEdgeDataBridge))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
