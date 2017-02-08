# -*- coding: utf-8 -*-
import unittest

from mock import patch
from openprocurement.edge.utils import get_now
from openprocurement.edge.tests.base import (
    AuctionBaseWebTest,
    test_auction_data,
    ROUTE_PREFIX
)


class AuctionResourceTest(AuctionBaseWebTest):

    def test_empty_listing(self):
        with patch('openprocurement.edge.utils.NestedMultiDict') as mock_dict:
            mock_dict.side_effect = UnicodeDecodeError('hitchhiker', "", 42, 43,
                                                       'the universe and everything else')
            response = self.app.get('/auctions', status=422)
        self.assertEqual(response.status, '422 Unprocessable Entity')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [{u'description': u'could not decode params',
                                                    u'location': u'body', u'name': u'data'}])
        self.assertEqual(response.content_type, 'application/json')

        with patch('openprocurement.edge.utils.NestedMultiDict') as mock_dict:
            mock_dict.side_effect = Exception('Very good exception')
            response = self.app.get('/auctions', status=422)
        self.assertEqual(response.status, '422 Unprocessable Entity')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [{u'description': u'Very good exception',
                                                    u'location': u'body', u'name': u'Exception'}])
        self.assertEqual(response.content_type, 'application/json')

        response = self.app.get('/auctions')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], [])
        self.assertNotIn('{\n    "', response.body)
        self.assertNotIn('callback({', response.body)
        self.assertEqual(response.json['next_page']['offset'], '')
        self.assertNotIn('prev_page', response.json)

        response = self.app.get('/auctions?opt_jsonp=callback')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertNotIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

        response = self.app.get('/auctions?opt_pretty=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('{\n    "', response.body)
        self.assertNotIn('callback({', response.body)

        response = self.app.get('/auctions?opt_jsonp=callback&opt_pretty=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

        response = self.app.get('/auctions?offset=2015-01-01T00:00:00+02:00&descending=1&limit=10')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], [])
        self.assertIn('descending=1', response.json['next_page']['uri'])
        self.assertIn('limit=10', response.json['next_page']['uri'])
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertIn('limit=10', response.json['prev_page']['uri'])

        response = self.app.get('/auctions?feed=changes')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], [])
        self.assertEqual(response.json['next_page']['offset'], '')
        self.assertNotIn('prev_page', response.json)

        response = self.app.get('/auctions?feed=changes&offset=0', status=404)
        self.assertEqual(response.status, '404 Not Found')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [
            {u'description': u'Offset expired/invalid', u'location': u'params', u'name': u'offset'}
        ])

        response = self.app.get('/auctions?feed=changes&descending=1&limit=10')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], [])
        self.assertIn('descending=1', response.json['next_page']['uri'])
        self.assertIn('limit=10', response.json['next_page']['uri'])
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertIn('limit=10', response.json['prev_page']['uri'])

    def test_listing(self):
        response = self.app.get('/auctions')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 0)

        auctions = []

        for i in range(3):
            offset = get_now().isoformat()
            auctions.append(self.create_auction())

        ids = ','.join([i['id'] for i in auctions])

        while True:
            response = self.app.get('/auctions')
            self.assertTrue(ids.startswith(','.join([i['id'] for i in response.json['data']])))
            if len(response.json['data']) == 3:
                break

        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]),
                         set([i['id'] for i in auctions]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]),
                         set([i['dateModified'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']],
                         sorted([i['dateModified'] for i in auctions]))

        while True:
            response = self.app.get('/auctions?offset={}'.format(offset))
            self.assertEqual(response.status, '200 OK')
            if len(response.json['data']) == 1:
                break
        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get('/auctions?limit=2')
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('prev_page', response.json)
        self.assertEqual(len(response.json['data']), 2)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 0)

        response = self.app.get('/auctions', params=[('opt_fields', 'status')])
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified', u'status']))
        self.assertIn('opt_fields=status', response.json['next_page']['uri'])

        response = self.app.get('/auctions', params=[('opt_fields', 'status,enquiryPeriod')])
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]),
                         set([u'id', u'dateModified', u'status', u'enquiryPeriod']))
        self.assertIn('opt_fields=status%2CenquiryPeriod', response.json['next_page']['uri'])

        response = self.app.get('/auctions?descending=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]),
                         set([i['id'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']],
                         sorted([i['dateModified'] for i in auctions], reverse=True))

        response = self.app.get('/auctions?descending=1&limit=2')
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 2)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 0)

        test_auction_data2 = test_auction_data.copy()
        test_auction_data2['mode'] = 'test'
        self.create_auction(test_auction_data2)

        while True:
            response = self.app.get('/auctions?mode=test')
            self.assertEqual(response.status, '200 OK')
            if len(response.json['data']) == 1:
                break

        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get('/auctions?mode=_all_')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 4)

    def test_listing_changes(self):
        response = self.app.get('/auctions?feed=changes')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 0)

        auctions = []

        for i in range(3):
            auctions.append(self.create_auction())

        ids = ','.join([i['id'] for i in auctions])

        while True:
            response = self.app.get('/auctions?feed=changes')
            self.assertTrue(ids.startswith(','.join([i['id'] for i in response.json['data']])))
            if len(response.json['data']) == 3:
                break

        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]),
                         set([i['id'] for i in auctions]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]),
                         set([i['dateModified'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']],
                         sorted([i['dateModified'] for i in auctions]))

        response = self.app.get('/auctions?feed=changes&limit=2')
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('prev_page', response.json)
        self.assertEqual(len(response.json['data']), 2)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 0)

        response = self.app.get('/auctions?feed=changes', params=[('opt_fields', 'status')])
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified', u'status']))
        self.assertIn('opt_fields=status', response.json['next_page']['uri'])

        response = self.app.get('/auctions?feed=changes',
                                params=[('opt_fields', 'status,enquiryPeriod')])
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]),
                         set([u'id', u'dateModified', u'status', u'enquiryPeriod']))
        self.assertIn('opt_fields=status%2CenquiryPeriod', response.json['next_page']['uri'])

        response = self.app.get('/auctions?feed=changes&descending=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]),
                         set([i['id'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']],
                         sorted([i['dateModified'] for i in auctions], reverse=True))

        response = self.app.get('/auctions?feed=changes&descending=1&limit=2')
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 2)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get(response.json['next_page']['path'].replace(ROUTE_PREFIX, ''))
        self.assertEqual(response.status, '200 OK')
        self.assertNotIn('descending=1', response.json['prev_page']['uri'])
        self.assertEqual(len(response.json['data']), 0)

        test_auction_data2 = test_auction_data.copy()
        test_auction_data2['mode'] = 'test'
        self.create_auction(test_auction_data2)

        while True:
            response = self.app.get('/auctions?feed=changes&mode=test')
            self.assertEqual(response.status, '200 OK')
            if len(response.json['data']) == 1:
                break
        self.assertEqual(len(response.json['data']), 1)

        response = self.app.get('/auctions?feed=changes&mode=_all_')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 4)

    def test_listing_draft(self):
        response = self.app.get('/auctions')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 0)

        auctions = []
        data = test_auction_data.copy()
        data.update({'status': 'draft'})

        for i in range(3):
            auctions.append(self.create_auction(data))

        ids = ','.join([i['id'] for i in auctions])

        while True:
            response = self.app.get('/auctions')
            self.assertTrue(ids.startswith(','.join([i['id'] for i in response.json['data']])))
            if len(response.json['data']) == 3:
                break

        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]),
                         set([i['id'] for i in auctions]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]),
                         set([i['dateModified'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']],
                         sorted([i['dateModified'] for i in auctions]))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AuctionResourceTest))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
