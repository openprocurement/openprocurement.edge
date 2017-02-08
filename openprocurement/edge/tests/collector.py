# -*- coding: utf-8 -*-
import unittest
from openprocurement.edge.collector import LogsCollector
from openprocurement.edge.collector import LogsCollectorConfigError
from mock import MagicMock, patch
from socket import error
from couchdb import Server, ResourceNotFound


class TestLogsCollector(unittest.TestCase):
    config = {
        'main': {
            'storage': 'couchdb',
            'couch_url': 'http://127.0.0.1:5984',
            'log_db': 'test_db'
        }
    }

    def tearDown(self):
        try:
            server = Server(self.config['main']['couch_url'])
            del server[self.config['main']['log_db']]
        except ResourceNotFound:
            pass

    def test_collector_init(self):
        test_config = {}
        with self.assertRaises(LogsCollectorConfigError):
            LogsCollector(test_config)
        test_config = {'main': {}}
        with self.assertRaises(LogsCollectorConfigError):
            LogsCollector(test_config)
        test_config['main'] = {
            'storage': 'couchdb'
        }
        with self.assertRaises(LogsCollectorConfigError):
            LogsCollector(test_config)
        test_config['main']['couch_url'] = 'labuda'
        with self.assertRaises(LogsCollectorConfigError):
            LogsCollector(test_config)
        test_config['main']['couch_url'] = 'http://127.0.0.1:5984'
        with self.assertRaises(LogsCollectorConfigError):
            LogsCollector(test_config)
        test_config['main']['log_db'] = 'test_db'
        try:
            server = Server(test_config['main']['couch_url'])
            del server[test_config['main']['log_db']]
        except ResourceNotFound:
            pass
        with patch('openprocurement.edge.collector.Server.create') as mock_create:
            mock_create.side_effect = error('test error')
            with self.assertRaises(LogsCollectorConfigError):
                LogsCollector(test_config)

    def test_save(self):
        log = LogsCollector(self.config)
        log.db.save = MagicMock()
        log.save({'a': 12, 'b': 13})

    def test_run(self):
        log = LogsCollector(self.config)
        log.run()


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestLogsCollector))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
