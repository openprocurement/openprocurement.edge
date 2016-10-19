# -*- coding: utf-8 -*-
import unittest

from openprocurement.edge.tests.base import test_tender_data, TenderBaseWebTest
import uuid
from openprocurement.edge.databridge import EdgeDataBridge
from mock import MagicMock, patch

import datetime
import io
import logging
import errno
from socket import error
from requests.exceptions import ConnectionError, MissingSchema
from couchdb import ResourceNotFound
# from openprocurement_client.sync import get_tenders

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
        # print bridge.db
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
        with self.assertRaises(AttributeError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object without variable 'tenders_api_server' in config
        test_config['main']={
            'tenders_api_ser': 'https://lb.api-sandbox.openprocurement.org'
        }
        with self.assertRaises(MissingSchema):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with empty tenders_api_server
        test_config['main'] = {'tenders_api_server': ''}
        with self.assertRaises(MissingSchema):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with wrong tenders_api_server
        test_config['main'] = {'tenders_api_server': 'https://lb.api-sandbox.openprocurement.or'}
        with self.assertRaises(ConnectionError):
            EdgeDataBridge(test_config)

        test_config['main']['tenders_api_server'] = 'https://lb.api-sandbox.openprocurement.org'

        # Create EdgeDataBridge object without 'couch_url'
        with self.assertRaises(ResourceNotFound):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with empty 'couch_url'
        test_config['main']['couch_url'] = ''
        with self.assertRaises(ResourceNotFound):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with wrong 'couch_url'
        test_config['main']['couch_url'] = 'http://localhost:598'
        with self.assertRaises(error):
            EdgeDataBridge(test_config)

        test_config['main']['couch_url'] = 'http://localhost:5984'

        # Create EdgeDataBridge object without 'public_db'
        with self.assertRaises(KeyError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with empty 'public_db'
        test_config['main']['public_db'] = ''
        with self.assertRaises(KeyError):
            EdgeDataBridge(test_config)

        # Create EdgeDataBridge object with wrong 'public_db'
        test_config['main']['public_db'] = 'wrong_db'
        with self.assertRaises(ResourceNotFound):
            EdgeDataBridge(test_config)


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


    def test_get_tenders_list(self):
        bridge = EdgeDataBridge(self.config)
        for tender_id, date_modified in bridge.get_teders_list():
            self.assertEqual(len(tender_id), 32)
            self.assertEqual(len(date_modified), 32)
            break



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestEdgeDataBridge))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
