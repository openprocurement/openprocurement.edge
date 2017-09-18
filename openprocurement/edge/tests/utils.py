# -*- coding: utf-8 -*-
import unittest
import logging
import os
import uuid
from couchdb import Server
from mock import patch, MagicMock
from openprocurement.edge.utils import (
    VALIDATE_BULK_DOCS_ID,
    VALIDATE_BULK_DOCS_UPDATE,
    prepare_couchdb,
    DataBridgeConfigError,
    route_prefix,
    push_views,
    update_logging_context,
    fix_url,
    VERSION
)

logger = logging.getLogger()
logger.level = logging.DEBUG


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.couch_url = 'http://127.0.0.1:5984'
        self.db_name = 'test_db'
        try:
            server = Server(self.couch_url)
            del server[self.db_name]
        except:
            pass

    def test_update_logging_context(self):
        request = MagicMock()
        request.get.return_value = None
        params = {'id': uuid.uuid4().hex, 'User-Agent': 'Best agent 6.0'}
        update_logging_context(request, params)
        self.assertEqual(request.logging_context['ID'], params['id'])
        self.assertEqual(request.logging_context['USER-AGENT'], params['User-Agent'])

    def test_fix_url(self):
        url = "/tenders/ttt/bids/bbb/documents/ddd?download=very_big_doc"
        item = {
            "format":
                "application/vnd.openxmlformats-officedocument.' \
                'wordprocessingml.document",
            "url": url
        }
        fix_url(item, 'https://public.api-sandbox.openprocurement.org')
        self.assertEqual(
            item['url'],
            'https://public.api-sandbox.openprocurement.org/api/{}'.format(
                VERSION) + url
        )

    def test_prepare_couchdb(self):
        # Database don't exist.
        server = Server(self.couch_url)
        self.assertNotIn(self.db_name, server)
        db = prepare_couchdb(self.couch_url, self.db_name, logger)
        self.assertIn(self.db_name, db.name)
        validate_func = db.get(VALIDATE_BULK_DOCS_ID).get('validate_doc_update')
        self.assertEqual(validate_func, VALIDATE_BULK_DOCS_UPDATE)

        # Database don't exist and create with exception
        del server[self.db_name]
        with patch('openprocurement.edge.utils.Server.create') as mock_create:
            mock_create.side_effect = DataBridgeConfigError('test error')
            with self.assertRaises(DataBridgeConfigError) as e:
                prepare_couchdb(self.couch_url, self.db_name, logger)
            self.assertEqual(e.exception.message, 'test error')

        self.assertNotIn(self.db_name, server)
        prepare_couchdb(self.couch_url, self.db_name, logger)
        self.assertIn(self.db_name, server)
        del server[self.db_name]

    def test_route_prefix(self):
        # Default value
        prefix = route_prefix()
        self.assertEqual(prefix, '/api/{}'.format(VERSION))
        prefix = route_prefix(settings={'api_version': 2.4})
        self.assertEqual(prefix, '/api/2.4')
        prefix = route_prefix(settings={'api_version': 'my version'})
        self.assertEqual(prefix, '/api/my version')

    def test_push_views(self):
        with self.assertRaises(Exception) as e:
            push_views()
        self.assertEqual(e.exception.message, 'Can\'t push couchapp. Please check '
                         '\'couchapp_path\' or \'couch_url\'.')
        server = Server(self.couch_url)
        db_name = 'test_' + uuid.uuid4().hex
        db = server.create(db_name)
        self.assertEqual(db.get('_design/auctions'), None)
        array_path = os.path.dirname(os.path.abspath(__file__)).split('/')
        app_path = ""
        for p in array_path[:-1]:
            app_path += p + '/'
        app_path += 'couch_views'
        push_views(couchapp_path=app_path + '/auctions', couch_url=self.couch_url + '/' + db_name)
        self.assertNotEqual(db.get('_design/auctions'), None)
        with self.assertRaises(DataBridgeConfigError) as e:
            push_views(couchapp_path='/haha', couch_url='')
        self.assertEqual(e.exception.message, 'Invalid path to couchapp.')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtils))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
