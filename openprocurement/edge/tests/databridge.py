# -*- coding: utf-8 -*-
import unittest

import datetime
import io
import logging
import uuid
from couchdb import Server, Database, Session
from gevent import sleep
from mock import MagicMock, patch
from munch import munchify
from openprocurement_client.exceptions import RequestFailed
from openprocurement.edge.tests.base import test_tender_data, TenderBaseWebTest
from openprocurement.edge.databridge import EdgeDataBridge, DataBridgeConfigError
from requests.exceptions import ConnectionError
from socket import error


logger = logging.getLogger()
logger.level = logging.DEBUG


class TestEdgeDataBridge(TenderBaseWebTest):
    config = {
        'main': {
            'resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'resources_api_version': "0",
            'public_resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
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
            'resource_items_queue_size': -1,
            'retry_resource_items_queue_size': -1,
            'retrievers_params': {
                'down_requests_sleep': 5,
                'up_requests_sleep': 1,
                'up_wait_sleep': 30,
                'queue_size': 101
            }
        },
        'version': 1
    }

    def tearDown(self):
        self.config['resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'
        self.config['resources_api_version'] = "0"
        self.config['public_resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'
        self.config['couch_url'] = 'http://localhost:5984'
        self.config['public_db'] = 'test_db'
        self.config['queue_size'] = 101
        self.config['api_clients_count'] = 3
        self.config['workers_count'] = 3
        self.config['retry_workers_count'] = 2
        self.config['filter_workers_count'] = 1
        self.config['retry_workers_count'] = 2
        self.config['retry_default_timeout'] = 5
        self.config['worker_sleep'] = 5
        self.config['watch_interval'] = 10
        self.config['queue_timeout'] = 5
        self.config['resource_items_queue_size'] = -1
        self.config['retry_resource_items_queue_size'] = -1
        try:
            server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
            del server[self.config['main']['public_db']]
        except:
            pass

    def test_init(self):
        bridge = EdgeDataBridge(self.config)
        self.assertIn('resources_api_server', bridge.config['main'])
        self.assertIn('resources_api_version', bridge.config['main'])
        self.assertIn('public_resources_api_server', bridge.config['main'])
        self.assertIn('couch_url', bridge.config['main'])
        self.assertIn('public_db', bridge.config['main'])
        self.assertEqual(self.config['main']['couch_url'], bridge.couch_url)

        del bridge
        self.config['main']['resource_items_queue_size'] = 101
        self.config['main']['retry_resource_items_queue_size'] = 101
        bridge = EdgeDataBridge(self.config)

        self.config['main']['couch_url'] = 'http://127.0.0.1:5987'
        with self.assertRaises(DataBridgeConfigError) as e:
            bridge = EdgeDataBridge(self.config)
        self.assertEqual(e.exception.message, 'Connection refused')

        del bridge
        self.config['main']['couch_url'] = 'http://127.0.0.1:5984'

        try:
            server = Server(self.config['main'].get('couch_url'))
            del server[self.config['main']['public_db']]
        except:
            pass
        test_config = {}

        # Create EdgeDataBridge object with wrong config variable structure
        test_config = {
           'mani': {
                'resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'resources_api_version': "0",
                'public_resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'couch_url': 'http://localhost:5984',
                'public_db': 'test_db',
            },
            'version': 1
        }
        with self.assertRaises(DataBridgeConfigError):
             EdgeDataBridge(test_config)

        # Create EdgeDataBridge object without variable 'resources_api_server' in config
        del test_config['mani']
        test_config['main'] = {}
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)
        with self.assertRaises(KeyError):
            test_config['main']['resources_api_server']

        # Create EdgeDataBridge object with empty resources_api_server
        test_config['main']['resources_api_server'] = ''
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with invalid resources_api_server
        test_config['main']['resources_api_server'] = 'my_server'
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        test_config['main']['resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        test_config['main']['public_db'] = 'public'
        test_config['main']['resources_api_version'] = "0"
        test_config['main']['public_resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        # Create EdgeDataBridge object with non exist database
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(bridge.db.name, test_config['main']['public_db'])

        try:
            server = Server(test_config['main'].get('couch_url') or 'http://127.0.0.1:5984')
            del server[test_config['main']['public_db']]
        except:
            pass

        with patch('openprocurement.edge.databridge.Server.create') as mock_create:
            mock_create.side_effect = error('test error')
            with self.assertRaises(DataBridgeConfigError) as e:
                bridge = EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with deleting config variables step by step
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError):
            test_config['main']['couch_url']
        del bridge

        del test_config['main']['resources_api_version']
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError):
            test_config['main']['resources_api_version']
        del bridge

        del test_config['main']['public_resources_api_server']
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError):
            test_config['main']['public_resources_api_server']
        del bridge
        server = Server(test_config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[test_config['main']['public_db']]

    @patch('openprocurement.edge.databridge.APIClient')
    def test_fill_api_clients_queue(self, mock_APIClient):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.api_clients_queue.qsize(), 0)
        bridge.fill_api_clients_queue()
        self.assertEqual(bridge.api_clients_queue.qsize(),
                         bridge.workers_min)

    @patch('openprocurement.edge.databridge.get_resource_items')
    def test_fill_resource_items_queue(self, mock_get_resource_items):
        bridge = EdgeDataBridge(self.config)
        db_dict_return = {
            'id':uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat()}
        bridge.db.get = MagicMock(side_effect=[None, db_dict_return])
        mock_get_resource_items.return_value = [db_dict_return,
                                                db_dict_return]
        self.assertEqual(bridge.resource_items_queue.qsize(), 0)
        self.assertEqual(bridge.log_dict['add_to_resource_items_queue'], 0)
        self.assertEqual(bridge.log_dict['skiped'], 0)
        bridge.fill_resource_items_queue()
        self.assertEqual(bridge.resource_items_queue.qsize(), 1)
        self.assertEqual(bridge.log_dict['add_to_resource_items_queue'], 1)
        self.assertEqual(bridge.log_dict['skiped'], 1)

    @patch('openprocurement.edge.databridge.spawn')
    @patch('openprocurement.edge.databridge.ResourceItemWorker.spawn')
    @patch('openprocurement.edge.databridge.APIClient')
    def test_gevent_watcher(self, mock_APIClient, mock_riw_spawn, mock_spawn):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.filter_workers_pool.free_count(), bridge.filter_workers_count)
        self.assertEqual(bridge.workers_pool.free_count(), bridge.workers_max)
        self.assertEqual(bridge.retry_workers_pool.free_count(), bridge.retry_workers_max)
        bridge.gevent_watcher()
        self.assertEqual(bridge.filter_workers_pool.free_count(), 0)
        self.assertEqual(bridge.workers_pool.free_count(), bridge.workers_max - bridge.workers_min)
        self.assertEqual(bridge.retry_workers_pool.free_count(), bridge.retry_workers_max - bridge.retry_workers_min)
        del bridge



    @patch('openprocurement.edge.databridge.APIClient')
    def test_create_api_client(self, mock_APIClient):
        mock_APIClient.side_effect = [RequestFailed(), munchify({
            'session': {'headers': {'User-Agent': 'test.agent'}}
        })]
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.api_clients_queue.qsize(), 0)
        self.assertEqual(bridge.log_dict['exceptions_count'], 0)
        bridge.create_api_client()
        self.assertEqual(bridge.log_dict['exceptions_count'], 1)
        self.assertEqual(bridge.api_clients_queue.qsize(), 1)

        del bridge


    def test_resource_items_filter(self):
        bridge = EdgeDataBridge(self.config)
        date_modified_old = datetime.datetime.utcnow().isoformat()
        date_modified_newest = datetime.datetime.utcnow().isoformat()
        side_effect = [{'dateModified': date_modified_old},
                       {'dateModified': date_modified_newest}, None,
                       Exception('db exception')]
        bridge.db.get = MagicMock(side_effect=side_effect)
        result = bridge.resource_items_filter(uuid.uuid4().hex,
                                              date_modified_newest)
        self.assertEqual(result, True)
        result = bridge.resource_items_filter(uuid.uuid4().hex,
                                              date_modified_old)
        self.assertEqual(result, False)
        result = bridge.resource_items_filter(uuid.uuid4().hex,
                                              date_modified_old)
        self.assertEqual(result, True)
        result = bridge.resource_items_filter(uuid.uuid4().hex,
                                              date_modified_old)
        self.assertEqual(result, True)


    def test_config_get(self):
        test_config = {
            'main': {
                'resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'resources_api_version': "0",
                'public_resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
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
