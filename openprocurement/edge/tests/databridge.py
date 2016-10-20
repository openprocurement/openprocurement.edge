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
from couchdb import Database, Session

logger = logging.getLogger()
logger.level = logging.DEBUG


class TestEdgeDataBridge(TenderBaseWebTest):
    config = {
        'main': {
            'tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'tenders_api_version': "0",
            'public_tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
            'couch_url': 'http://localhost:5984',
            'public_db': 'public_db'
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
        self.assertEqual(self.config['main']['couch_url'] + '/' + self.config['main']['public_db'], bridge.couch_url)

        test_config = {}

        # Create EdgeDataBridge object with wrong config variable structure
        test_config = {
           'mani': {
                'tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'tenders_api_version': "0",
                'public_tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'couch_url': 'http://localhost:5984',
                'public_db': 'public_db'
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

        # Create EdgeDataBridge object with wrong tenders_api_server
        test_config['main']['tenders_api_server'] = 'https://lb.api-sandbox.openprocurement.or'
        with self.assertRaises(ConnectionError):
            EdgeDataBridge(test_config)

        test_config['main']['tenders_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        # Create EdgeDataBridge object without 'couch_url'
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)
        with self.assertRaises(KeyError):
            test_config['main']['couch_url']

        # Create EdgeDataBridge object with empty 'couch_url'
        test_config['main']['couch_url'] = ''
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with wrong 'couch_url'
        test_config['main']['couch_url'] = 'http://localhost:598'
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        test_config['main']['couch_url'] = 'http://localhost:5984'

        # Create EdgeDataBridge object without 'public_db'
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)
        with self.assertRaises(KeyError):
            test_config['main']['public_db']

        # Create EdgeDataBridge object with empty 'public_db'
        test_config['main']['public_db'] = ''
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with wrong 'public_db'
        test_config['main']['public_db'] = 'wrong_db'
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)

        test_config['main']['public_db'] = 'public_db'
        test_config['main']['tenders_api_version'] = "0"
        test_config['main']['public_tenders_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        # Create EdgeDataBridge object with deleting config variables step by step
        del test_config['main']['couch_url']
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

        del test_config['main']['public_db']
        with self.assertRaises(DataBridgeConfigError):
            EdgeDataBridge(test_config)
        with self.assertRaises(KeyError):
            test_config['main']['public_db']


    def test_save_tender_in_db(self):
        log_string = io.BytesIO()
        stream_handler = logging.StreamHandler(log_string)
        logger.addHandler(stream_handler)

        bridge = EdgeDataBridge(self.config)
        mock_tender = {'data': test_tender_data}
        bridge.client.get_tender = MagicMock(return_value=mock_tender)

        # Save tender

        tid = uuid.uuid4().hex
        t_date_modified = datetime.datetime.utcnow().isoformat()
        mock_tender['data']['dateModified'] = t_date_modified
        bridge.save_tender_in_db(tid, t_date_modified)
        x = log_string.getvalue().split('\n')
        self.assertEqual(x[1].strip(), 'Save tender ' + tid)
        tender_in_db = bridge.db.get(tid)
        self.assertEqual(tender_in_db.id, tid)

        # Tender exist in db and not modified
        result = bridge.save_tender_in_db(tid, t_date_modified)
        self.assertEqual(result, None)

        # Update tender
        t_date_modified = datetime.datetime.utcnow().isoformat()
        mock_tender['data']['dateModified'] = t_date_modified
        bridge.save_tender_in_db(tid, t_date_modified)
        x = log_string.getvalue().split('\n')
        self.assertEqual(x[2].strip(), 'Update tender ' + tid)
        updated_tender = bridge.db.get(tid)
        self.assertEqual(updated_tender['dateModified'], unicode(t_date_modified))

        # Tender not found
        bridge.client.get_tender = MagicMock(return_value=test_tender_data)
        bridge.save_tender_in_db(tid, datetime.datetime.utcnow().isoformat())
        x = log_string.getvalue().split('\n')
        self.assertEqual(x[3].strip(), 'Tender ' + tid + ' not found')
        bridge.db.delete(updated_tender)

        # Saving tender with exception
        bridge.client.get_tender = MagicMock(return_value=mock_tender)
        bridge.config['main']['couch_url'] = ''
        bridge.config['main']['public_db'] = ''
        bridge.db = Database('bridge.couch_url',
                            session=Session(retry_delays=range(10)))
        new_mock_tender = mock_tender
        new_mock_tender['dateModified'] = datetime.datetime.utcnow().isoformat()
        new_mock_tender['_rev'] = '2-' + uuid.uuid4().hex
        bridge.db.get = MagicMock(return_value=new_mock_tender)
        tid = uuid.uuid4().hex
        bridge.save_tender_in_db(tid, datetime.datetime.utcnow().isoformat())
        x = log_string.getvalue().split('\n')
        self.assertEqual(x[5].strip(), 'Saving tender ' + tid + ' fail with error (400, (u\'illegal_database_name\', u"Name: \'bridge.couch_url\'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."))')

        logger.removeHandler(stream_handler)
        log_string.close()

    def test_run(self):
        log_string = io.BytesIO()
        stream_handler = logging.StreamHandler(log_string)
        logger.addHandler(stream_handler)

        bridge = EdgeDataBridge(self.config)
        mock_tender = {'data': test_tender_data}
        bridge.client.get_tender = MagicMock(return_value=mock_tender)
        tid = uuid.uuid4().hex
        t_date_modified = datetime.datetime.utcnow().isoformat()
        mock_tender['data']['dateModified'] = t_date_modified
        bridge.save_tender_in_db(tid, t_date_modified)
        bridge.get_teders_list = MagicMock(return_value=[[tid, datetime.datetime.utcnow().isoformat()]])
        bridge.run()
        x = log_string.getvalue().split('\n')
        self.assertEqual(x[2], 'Start Edge Bridge')
        self.assertEqual(x[3], 'Start data sync...')
        del_tender = bridge.db.get(tid)
        bridge.db.delete(del_tender)

        logger.removeHandler(stream_handler)
        log_string.close()

    @patch('openprocurement.edge.databridge.get_tenders')
    def test_get_tenders_list(self, mock_get_tenders):
        tid = uuid.uuid4().hex
        t_date_modified =  datetime.datetime.utcnow().isoformat()
        mock_get_tenders.return_value = [{'id': tid, 'dateModified': t_date_modified}]
        bridge = EdgeDataBridge(self.config)
        for tender_id, date_modified in bridge.get_teders_list():
            self.assertEqual(tender_id, tid)
            self.assertEqual(t_date_modified, date_modified)

    def test_config_get(self):
        test_config = {
            'main': {
                'tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'tenders_api_version': "0",
                'public_tenders_api_server': 'https://lb.api-sandbox.openprocurement.org',
                'couch_url': 'http://localhost:5984',
                'public_db': 'public_db'
            },
            'version': 1
        }

        bridge = EdgeDataBridge(test_config)
        couch_url_config = bridge.config_get('couch_url')
        self.assertEqual(couch_url_config, test_config['main']['couch_url'])

        del bridge.config['main']['couch_url']
        couch_url_config = bridge.config_get('couch_url')
        self.assertEqual(couch_url_config, None)

        del bridge.config['main']
        with self.assertRaises(DataBridgeConfigError):
            bridge.config_get('couch_url')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestEdgeDataBridge))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
