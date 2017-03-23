# -*- coding: utf-8 -*-
import unittest

import datetime
import os
import logging
import uuid
from gevent import sleep
from gevent.queue import Queue
from couchdb import Server
from mock import MagicMock, patch
from munch import munchify
from httplib import IncompleteRead
from openprocurement_client.exceptions import RequestFailed
from openprocurement.edge.tests.base import TenderBaseWebTest
from openprocurement.edge.databridge import EdgeDataBridge
from openprocurement.edge.utils import (
    DataBridgeConfigError,
    push_views,
    VALIDATE_BULK_DOCS_ID,
    VALIDATE_BULK_DOCS_UPDATE
)


logger = logging.getLogger()
logger.level = logging.DEBUG


class AlmostAlwaysTrue(object):

    def __init__(self, total_iterations=1):
        self.total_iterations = total_iterations
        self.current_iteration = 0

    def __nonzero__(self):
        if self.current_iteration < self.total_iterations:
            self.current_iteration += 1
            return bool(1)
        return bool(0)


class TestEdgeDataBridge(TenderBaseWebTest):
    config = {
        'main': {
            'resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'resources_api_version': "0",
            'public_resources_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'couch_url': 'http://localhost:5984',
            'db_name': 'test_db',
            'queue_size': 101,
            'api_clients_count': 3,
            'workers_min': 0,
            'workers_max': 2,
            'retry_workers_count': 2,
            'filter_workers_count': 1,
            'retry_workers_count': 2,
            'retry_default_timeout': 5,
            'worker_sleep': 5,
            'watch_interval': 0.1,
            'queue_timeout': 5,
            'resource_items_queue_size': -1,
            'retry_resource_items_queue_size': -1,
            'bulk_query_limit': 1,
            'retrieve_mode': '_all_',
            'perfomance_window': 0.1,
            'queues_controller_timeout': 0.01,
            'retrievers_params': {
                'down_requests_sleep': 5,
                'up_requests_sleep': 1,
                'up_wait_sleep': 30,
                'queue_size': 101
            }
        },
        'version': 1
    }

    def setUp(self):
        server = Server(self.config['main']['couch_url'])
        if self.config['main']['db_name'] in server:
            self.db = server[self.config['main']['db_name']]
        else:
            self.db = server.create(self.config['main']['db_name'])
        array_path = os.path.dirname(os.path.abspath(__file__)).split('/')
        app_path = ""
        for p in array_path[:-1]:
            app_path += p + '/'
        app_path += 'couch_views'
        couchdb_url = self.config['main']['couch_url'] \
            + '/' + self.config['main']['db_name']
        for resource in ('/tenders', '/plans', '/contracts', '/auctions'):
            push_views(couchapp_path=app_path + resource,
                       couch_url=couchdb_url)
        validate_doc = {
            '_id': VALIDATE_BULK_DOCS_ID,
            'validate_doc_update': VALIDATE_BULK_DOCS_UPDATE
        }
        try:
            self.db.save(validate_doc)
        except Exception:
            pass

    def tearDown(self):
        self.config['resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'
        self.config['resources_api_version'] = "0"
        self.config['public_resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'
        self.config['couch_url'] = 'http://localhost:5984'
        self.config['db_name'] = 'test_db'
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
        self.config['bulk_query_limit'] = 1
        try:
            server = Server(self.config['main'].get('couch_url') or 'http://127.0.0.1:5984')
            del server[self.config['main']['db_name']]
        except:
            pass

    def test_init(self):
        bridge = EdgeDataBridge(self.config)
        self.assertIn('resources_api_server', bridge.config['main'])
        self.assertIn('resources_api_version', bridge.config['main'])
        self.assertIn('public_resources_api_server', bridge.config['main'])
        self.assertIn('couch_url', bridge.config['main'])
        self.assertIn('db_name', bridge.config['main'])
        self.assertEqual(self.config['main']['couch_url'], bridge.couch_url)
        self.assertEqual(len(bridge.server.uuids()[0]), 32)

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
            del server[self.config['main']['db_name']]
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
                'db_name': 'test_db',
                'retrievers_params': {
                    'down_requests_sleep': 5,
                    'up_requests_sleep': 1,
                    'up_wait_sleep': 30,
                    'queue_size': 101
                }
            },
            'version': 1
        }
        with self.assertRaises(DataBridgeConfigError) as e:
            EdgeDataBridge(test_config)
        self.assertEqual(e.exception.message, 'In config dictionary missed '
                         'section \'main\'')

        # Create EdgeDataBridge object without variable 'resources_api_server' in config
        del test_config['mani']
        test_config['main'] = {
            'retrievers_params': {
                'down_requests_sleep': 5,
                'up_requests_sleep': 1,
                'up_wait_sleep': 30,
                'queue_size': 101
            }
        }
        with self.assertRaises(DataBridgeConfigError) as e:
            EdgeDataBridge(test_config)
        self.assertEqual(e.exception.message, 'In config dictionary empty or '
                         'missing \'tenders_api_server\'')
        with self.assertRaises(KeyError) as e:
            test_config['main']['resources_api_server']
        self.assertEqual(e.exception.message, 'resources_api_server')

        # Create EdgeDataBridge object with empty resources_api_server
        test_config['main']['resources_api_server'] = ''
        with self.assertRaises(DataBridgeConfigError) as e:
            EdgeDataBridge(test_config)
        self.assertEqual(e.exception.message, 'In config dictionary empty or '
                         'missing \'tenders_api_server\'')

        # Create EdgeDataBridge object with invalid resources_api_server
        test_config['main']['resources_api_server'] = 'my_server'
        with self.assertRaises(DataBridgeConfigError) as e:
            EdgeDataBridge(test_config)
        self.assertEqual(e.exception.message, 'Invalid \'tenders_api_server\' '
                         'url.')

        test_config['main']['resources_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        test_config['main']['db_name'] = 'public'
        test_config['main']['resources_api_version'] = "0"
        test_config['main']['public_resources_api_server'] \
            = 'https://lb.api-sandbox.openprocurement.org'

        # Create EdgeDataBridge object with deleting config variables step by step
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError) as e:
            test_config['main']['couch_url']
        self.assertEqual(e.exception.message, 'couch_url')
        del bridge

        del test_config['main']['resources_api_version']
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError) as e:
            test_config['main']['resources_api_version']
        self.assertEqual(e.exception.message, 'resources_api_version')
        del bridge

        del test_config['main']['public_resources_api_server']
        bridge = EdgeDataBridge(test_config)
        self.assertEqual(type(bridge), EdgeDataBridge)
        with self.assertRaises(KeyError) as e:
            test_config['main']['public_resources_api_server']
        self.assertEqual(e.exception.message, 'public_resources_api_server')
        del bridge
        server = Server(test_config['main'].get('couch_url') or 'http://127.0.0.1:5984')
        del server[test_config['main']['db_name']]

        test_config['main']['retrievers_params']['up_wait_sleep'] = 0
        with self.assertRaises(DataBridgeConfigError) as e:
            EdgeDataBridge(test_config)
        self.assertEqual(e.exception.message, 'Invalid \'up_wait_sleep\' in '
                         '\'retrievers_params\'. Value must be grater than 30.')

    @patch('openprocurement.edge.databridge.APIClient')
    def test_fill_api_clients_queue(self, mock_APIClient):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.api_clients_queue.qsize(), 0)
        bridge.fill_api_clients_queue()
        self.assertEqual(bridge.api_clients_queue.qsize(),
                         bridge.workers_min)

    @patch('openprocurement.edge.databridge.ResourceFeeder.get_resource_items')
    def test_fill_resource_items_queue(self, mock_get_resource_items):
        bridge = EdgeDataBridge(self.config)
        db_dict_list = [
            {
                'id': uuid.uuid4().hex,
                'dateModified': datetime.datetime.utcnow().isoformat()
            },
            {
                'id': uuid.uuid4().hex,
                'dateModified': datetime.datetime.utcnow().isoformat()
            }]
        bridge.db.get = MagicMock(side_effect=[None, db_dict_list[0]])
        mock_get_resource_items.return_value = db_dict_list
        self.assertEqual(bridge.resource_items_queue.qsize(), 0)
        self.assertEqual(bridge.log_dict['add_to_resource_items_queue'], 0)
        self.assertEqual(bridge.log_dict['skiped'], 0)
        view_return_list = [
            munchify({
                'id': db_dict_list[0]['id'],
                'key': db_dict_list[0]['dateModified']
            })
        ]
        bridge.db.view = MagicMock(return_value=view_return_list)
        bridge.fill_resource_items_queue()
        self.assertEqual(bridge.resource_items_queue.qsize(), 1)
        self.assertEqual(bridge.log_dict['add_to_resource_items_queue'], 1)
        self.assertEqual(bridge.log_dict['skiped'], 1)

        bridge.db.view = MagicMock(side_effect=IncompleteRead('test'))
        with self.assertRaises(IncompleteRead) as e:
            bridge.fill_resource_items_queue()
        self.assertEqual(e.exception.args[0], 'test')

    @patch('openprocurement.edge.databridge.spawn')
    @patch('openprocurement.edge.databridge.ResourceItemWorker.spawn')
    @patch('openprocurement.edge.databridge.APIClient')
    def test_gevent_watcher(self, mock_APIClient, mock_riw_spawn, mock_spawn):
        bridge = EdgeDataBridge(self.config)
        return_dict = {
            'type': 'indexer',
            'database': bridge.db_name,
            'design_document': '_design/{}'.format(bridge.workers_config['resource']),
            'progress': 99
        }
        bridge.server.tasks = MagicMock(return_value=[return_dict])
        self.assertEqual(bridge.filter_workers_pool.free_count(),
                         bridge.filter_workers_count)
        self.assertEqual(bridge.workers_pool.free_count(),
                         bridge.workers_max)
        self.assertEqual(bridge.retry_workers_pool.free_count(),
                         bridge.retry_workers_max)
        bridge.gevent_watcher()
        self.assertEqual(bridge.filter_workers_pool.free_count(), 0)
        self.assertEqual(bridge.workers_pool.free_count(),
                         bridge.workers_max - bridge.workers_min)
        self.assertEqual(bridge.retry_workers_pool.free_count(),
                         bridge.retry_workers_max - bridge.retry_workers_min)
        del bridge

    @patch('openprocurement.edge.databridge.APIClient')
    @patch('openprocurement.edge.databridge.ResourceItemWorker.spawn')
    def test_queues_controller(self, mock_riw_spawn, mock_APIClient):
        bridge = EdgeDataBridge(self.config)
        bridge.resource_items_queue_size = 10
        bridge.resource_items_queue = Queue(10)
        for i in xrange(0, 10):
            bridge.resource_items_queue.put('a')
        self.assertEqual(len(bridge.workers_pool), 0)
        self.assertEqual(bridge.resource_items_queue.qsize(), 10)
        with patch('__builtin__.True', AlmostAlwaysTrue()):
            bridge.queues_controller()
        self.assertEqual(len(bridge.workers_pool), 1)
        bridge.workers_pool.add(mock_riw_spawn)
        self.assertEqual(len(bridge.workers_pool), 2)

        for i in xrange(0, 10):
            bridge.resource_items_queue.get()
        with patch('__builtin__.True', AlmostAlwaysTrue()):
            bridge.queues_controller()
        self.assertEqual(len(bridge.workers_pool), 1)
        self.assertEqual(bridge.resource_items_queue.qsize(), 0)

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
                'db_name': 'test_db',
                'retrievers_params': {
                    'down_requests_sleep': 5,
                    'up_requests_sleep': 1,
                    'up_wait_sleep': 30,
                    'queue_size': 101
                }
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
        del server[test_config['main']['db_name']]

        del bridge.config['main']
        with self.assertRaises(DataBridgeConfigError):
            bridge.config_get('couch_url')

    def test__get_average_request_duration(self):
        bridge = EdgeDataBridge(self.config)
        bridge.create_api_client()
        bridge.create_api_client()
        bridge.create_api_client()
        res, _ = bridge._get_average_requests_duration()
        self.assertEqual(res, 0)
        request_duration = 1
        for k in bridge.api_clients_info:
            for i in xrange(0, 3):
                bridge.api_clients_info[k]['request_durations'][datetime.datetime.now()] =\
                    request_duration
            request_duration += 1
        res, res_list = bridge._get_average_requests_duration()
        self.assertEqual(res, 2)
        self.assertEqual(len(res_list), 3)

        delta = datetime.timedelta(seconds=301)
        grown_date = datetime.datetime.now() - delta
        bridge.api_clients_info[uuid.uuid4().hex] = {
            'request_durations': {grown_date: 1},
            'destroy': False,
            'request_interval': 0,
            'avg_duration': 0
        }
        self.assertEqual(len(bridge.api_clients_info), 4)

        res, res_list = bridge._get_average_requests_duration()
        grown = 0
        for k in bridge.api_clients_info:
            if bridge.api_clients_info[k].get('grown', False):
                grown += 1
        self.assertEqual(res, 1.75)
        self.assertEqual(len(res_list), 4)
        self.assertEqual(grown, 1)

    def test_bridge_stats(self):
        bridge = EdgeDataBridge(self.config)
        bridge.feeder = MagicMock()
        bridge.feeder.queue.qsize.return_value = 44
        bridge.feeder.backward_info = {
            'last_response': datetime.datetime.now(),
            'status': 1,
            'resource_item_count': 13
        }
        bridge.feeder.forward_info = {
            'last_response': datetime.datetime.now(),
            'resource_item_count': 31
        }
        sleep(1)
        res = bridge.bridge_stats()
        keys = ['resource_items_queue_size', 'retry_resource_items_queue_size', 'workers_count',
                'api_clients_count', 'avg_request_duration', 'filter_workers_count',
                'retry_workers_count', 'min_avg_request_duration', 'max_avg_request_duration']
        for k, v in bridge.log_dict.items():
            self.assertEqual(res[k], v)
        for k in keys:
            self.assertEqual(res[k], 0)
        self.assertEqual(res['sync_backward_response_len'],
                         bridge.feeder.backward_info['resource_item_count'])
        self.assertEqual(res['sync_forward_response_len'],
                         bridge.feeder.forward_info['resource_item_count'])
        self.assertGreater(res['vms'], 0)
        self.assertGreater(res['rss'], 0)
        self.assertGreater(res['sync_backward_last_response'], 0)
        self.assertGreater(res['sync_forward_last_response'], 0)
        self.assertEqual(res['sync_queue'], 44)
        self.assertEqual(res['resource'], bridge.workers_config['resource'])
        self.assertEqual(res['_id'], bridge.workers_config['resource'])
        self.assertNotEqual(res['time'], '')

        bridge.feeder.backward_info['status'] = 0
        bridge.api_clients_info['c1'] = {'request_durations': {datetime.datetime.now(): 1}}
        bridge.api_clients_info['c2'] = {'request_durations': {datetime.datetime.now(): 2}}

        res = bridge.bridge_stats()
        self.assertEqual(res['sync_backward_last_response'], 0)
        self.assertNotEqual(res['max_avg_request_duration'], 0)
        self.assertNotEqual(res['min_avg_request_duration'], 0)

    def test__calculate_st_dev(self):
        bridge = EdgeDataBridge(self.config)
        values = [1.1, 1.11, 1.12, 1.13, 1.14]
        stdev = bridge._calculate_st_dev(values)
        self.assertEqual(stdev, 0.014)
        stdev = bridge._calculate_st_dev([])
        self.assertEqual(stdev, 0)

    def test__mark_bad_clients(self):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(bridge.api_clients_queue.qsize(), 0)
        self.assertEqual(len(bridge.api_clients_info), 0)

        bridge.create_api_client()
        bridge.create_api_client()
        bridge.create_api_client()
        self.assertEqual(len(bridge.api_clients_info), 3)
        avg_duration = 1
        req_intervals = [0, 2, 0, 0]
        for cid in bridge.api_clients_info:
            self.assertEqual(bridge.api_clients_info[cid]['destroy'], False)
            bridge.api_clients_info[cid]['avg_duration'] = avg_duration
            bridge.api_clients_info[cid]['grown'] = True
            bridge.api_clients_info[cid]['request_interval'] = req_intervals[avg_duration]
            avg_duration += 1
        avg = 1.5
        bridge._mark_bad_clients(avg)
        self.assertEqual(len(bridge.api_clients_info), 6)
        self.assertEqual(bridge.api_clients_queue.qsize(), 6)
        to_destroy = 0
        for cid in bridge.api_clients_info:
            if bridge.api_clients_info[cid]['destroy']:
                to_destroy += 1
        self.assertEqual(to_destroy, 3)

    def test_perfomance_watcher(self):
        bridge = EdgeDataBridge(self.config)
        for i in xrange(0, 3):
            bridge.create_api_client()
        req_duration = 1
        for _, info in bridge.api_clients_info.items():
            info['request_durations'][datetime.datetime.now()] = req_duration
            req_duration += 1
            self.assertEqual(info.get('grown', False), False)
            self.assertEqual(len(info['request_durations']), 1)
        self.assertEqual(len(bridge.api_clients_info), 3)
        self.assertEqual(bridge.api_clients_queue.qsize(), 3)
        sleep(1)

        bridge.perfomance_watcher()
        grown = 0
        new_clients = 0
        for cid, info in bridge.api_clients_info.items():
            if info.get('grown', False):
                grown += 1
            elif not info.get('grown', False) and not info['destroy']:
                new_clients += 1
            self.assertEqual(len(info['request_durations']), 0)
        self.assertEqual(len(bridge.api_clients_info), 3)
        self.assertEqual(bridge.api_clients_queue.qsize(), 3)
        self.assertEqual(grown, 2)
        self.assertEqual(new_clients, 1)

    @patch('openprocurement.edge.databridge.EdgeDataBridge.fill_resource_items_queue')
    @patch('openprocurement.edge.databridge.EdgeDataBridge.queues_controller')
    @patch('openprocurement.edge.databridge.EdgeDataBridge.perfomance_watcher')
    @patch('openprocurement.edge.databridge.EdgeDataBridge.gevent_watcher')
    def test_run(self, mock_gevent, mock_perfomance, mock_controller, mock_fill):
        bridge = EdgeDataBridge(self.config)
        self.assertEqual(len(bridge.filter_workers_pool), 0)
        with patch('__builtin__.True', AlmostAlwaysTrue(4)):
            bridge.run()
        self.assertEqual(mock_fill.call_count, 1)
        self.assertEqual(mock_controller.call_count, 1)
        self.assertEqual(mock_gevent.call_count, 1)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestEdgeDataBridge))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
