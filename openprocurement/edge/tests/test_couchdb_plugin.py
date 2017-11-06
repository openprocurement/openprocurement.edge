# -*- coding: utf-8 -*-
import unittest
from uuid import uuid4
from copy import deepcopy
from mock import patch, MagicMock
from openprocurement.edge.storages.couchdb_plugin import CouchDBStorage


class TestCouchDBStorage(unittest.TestCase):

    storage_conf = {
        'storage': {
            'host': '127.0.0.1',
            'port': 5984,
            'user': 'john',
            'password': 'smith'
        }
    }

    @patch('openprocurement.edge.utils.dispatch')
    @patch('openprocurement.edge.utils.Server')
    def test_init(self, mocked_server, mocked_dispatch):
        db = CouchDBStorage(self.storage_conf, 'tenders')
        self.assertEqual(
            db.couch_url,
            'http://{user}:{password}@{host}:{port}'.format(
                **self.storage_conf['storage'])
        )

    @patch('openprocurement.edge.utils.dispatch')
    @patch('openprocurement.edge.utils.Server')
    def test_get_doc(self, mocked_server, mocked_dispatch):
        config = deepcopy(self.storage_conf)
        del config['storage']['user']
        del config['storage']['password']
        db = CouchDBStorage(config, 'tenders')
        db.db = MagicMock()
        mocked_doc = {
            'id': '1',
            '_id': '1',
            'doc_type': 'Tender',
            '_rev': '1-{}'.format(uuid4().hex)
        }
        db.db.get.return_value = mocked_doc
        doc = db.get_doc('1')
        self.assertEqual(mocked_doc, doc)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCouchDBStorage))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')