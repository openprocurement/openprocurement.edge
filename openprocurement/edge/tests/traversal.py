# -*- coding: utf-8 -*-
import unittest
from mock import MagicMock, patch
from munch import munchify
from openprocurement.edge.traversal import Root


class TestTraversal(unittest.TestCase):

    def test_Root(self):
        request = munchify({'registry': {'db': 'database'}})
        root = Root(request)
        self.assertEqual(root.request, request)
        self.assertEqual(root.db, request.registry.db)

    def test_get_item(self):
        pass

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestTraversal))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
