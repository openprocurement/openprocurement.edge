# -*- coding: utf-8 -*-
import unittest
from uuid import uuid4
from copy import deepcopy

from openprocurement.api.models import get_now
from openprocurement.edge.tests.base import AuctionBaseWebTest, test_award, test_auction_data, test_document, ROUTE_PREFIX
try:
    import openprocurement.auctions.core as auctions_core
except ImportError:
     auctions_core = None

@unittest.skipUnless(auctions_core, "Auctions is not reachable")
class AuctionResourceTest(AuctionBaseWebTest):

    def test_empty_listing(self):
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
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in auctions]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]), set([i['dateModified'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']], sorted([i['dateModified'] for i in auctions]))


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
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified', u'status', u'enquiryPeriod']))
        self.assertIn('opt_fields=status%2CenquiryPeriod', response.json['next_page']['uri'])

        response = self.app.get('/auctions?descending=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']], sorted([i['dateModified'] for i in auctions], reverse=True))

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
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in auctions]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]), set([i['dateModified'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']], sorted([i['dateModified'] for i in auctions]))

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

        response = self.app.get('/auctions?feed=changes', params=[('opt_fields', 'status,enquiryPeriod')])
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified', u'status', u'enquiryPeriod']))
        self.assertIn('opt_fields=status%2CenquiryPeriod', response.json['next_page']['uri'])

        response = self.app.get('/auctions?feed=changes&descending=1')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(len(response.json['data']), 3)
        self.assertEqual(set(response.json['data'][0]), set([u'id', u'dateModified']))
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']], sorted([i['dateModified'] for i in auctions], reverse=True))

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
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in auctions]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]), set([i['dateModified'] for i in auctions]))
        self.assertEqual([i['dateModified'] for i in response.json['data']], sorted([i['dateModified'] for i in auctions]))

    def test_get_auction(self):
        auction = self.create_auction()

        response = self.app.get('/auctions/{}'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertDictEqual(response.json['data'], auction)

        response = self.app.get('/auctions/{}?opt_jsonp=callback'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('callback({"data": {"', response.body)

        response = self.app.get('/auctions/{}?opt_pretty=1'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('{\n    "data": {\n        "', response.body)

    def test_auction_not_found(self):
        response = self.app.get('/auctions')
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), 0)

        response = self.app.get('/auctions/some_id', status=404)
        self.assertEqual(response.status, '404 Not Found')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [
            {u'description': u'Not Found', u'location': u'url', u'name': u'auction_id'}
        ])

        response = self.app.patch_json(
            '/auctions/some_id', {'data': {}}, status=404)
        self.assertEqual(response.status, '404 Not Found')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [
            {u'description': u'Not Found', u'location': u'url', u'name': u'auction_id'}
        ])

        # put custom document object into database to check auction construction on non-Auction data
        data = {'contract': 'test', '_id': uuid4().hex}
        self.db.save(data)

        response = self.app.get('/auctions/{}'.format(data['_id']), status=404)
        self.assertEqual(response.status, '404 Not Found')


@unittest.skipUnless(auctions_core, "Auctions is not reachable")
class AuctionAwardResourceTest(AuctionBaseWebTest):

    def test_listing(self):
        auction = self.create_auction()

        response = self.app.get('/auctions/{}/awards'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], auction['awards'])
        self.assertNotIn('{\n    "', response.body)
        self.assertNotIn('callback({', response.body)

        response = self.app.get('/auctions/{}/awards?opt_jsonp=callback'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertNotIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

        response = self.app.get('/auctions/{}/awards?opt_pretty=1'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('{\n    "', response.body)
        self.assertNotIn('callback({', response.body)

        response = self.app.get('/auctions/{}/awards?opt_jsonp=callback&opt_pretty=1'.format(auction['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

    def test_listing_changes(self):

        auction = self.create_auction()

        data = self.db[auction['id']]

        awards = data['awards']

        for i in range(3):
            award = deepcopy(test_award)
            award['date'] = get_now().isoformat()
            award['id'] = uuid4().hex
            awards.append(award)

        self.db.save(data)

        ids = ','.join([i['id'] for i in awards])

        response = self.app.get('/auctions/{}/awards'.format(auction['id']))
        self.assertTrue(ids.startswith(','.join([i['id'] for i in response.json['data']])))


        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), len(awards))
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in awards]))
        self.assertEqual(set([i['date'] for i in response.json['data']]), set([i['date'] for i in awards]))
        self.assertEqual([i['date'] for i in response.json['data']], sorted([i['date'] for i in awards]))

    def test_get_award(self):
        auction = self.create_auction()

        award = auction['awards'][0]

        response = self.app.get('/auctions/{}/awards/{}'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertDictEqual(response.json['data'], award)

        response = self.app.get('/auctions/{}/awards/{}?opt_jsonp=callback'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('callback({"data": {"', response.body)

        response = self.app.get('/auctions/{}/awards/{}?opt_pretty=1'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('{\n    "data": {\n        "', response.body)

    def test_award_not_found(self):
        auction = self.create_auction()

        response = self.app.get('/auctions/{}/awards/some_id'.format(auction['id']), status=404)
        self.assertEqual(response.status, '404 Not Found')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [
            {u'description': u'Not Found', u'location': u'url', u'name': u'award_id'}
        ])

    def test_get_document_with_versions(self):
        auction = self.create_auction()
        data = self.db[auction['id']]
        documents = data['documents']

        for i in range(3):
            document = deepcopy(test_document)
            document['id'] = data['documents'][0]['id']
            document['url'] += str(i)
            document['dateModified'] = get_now().isoformat()
            documents.append(document)

        self.db.save(data)

        versions = [{'dateModified': i['dateModified'], 'url': i['url']} for i in documents[:-1]]

        response = self.app.get('/auctions/{}/documents/{}'.format(auction['id'], document['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(len(response.json['data']['previousVersions']), len(versions))
        self.assertEqual(response.json['data']['previousVersions'], versions)

@unittest.skipUnless(auctions_core, "Auctions is not reachable")
class AuctionAwardDocumentResourceTest(AuctionBaseWebTest):

    def test_listing(self):
        auction = self.create_auction()
        award = auction['awards'][0]
        document = award['documents'][0]

        response = self.app.get('/auctions/{}/awards/{}/documents'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['data'], award['documents'])
        self.assertNotIn('{\n    "', response.body)
        self.assertNotIn('callback({', response.body)

        response = self.app.get('/auctions/{}/awards/{}/documents?opt_jsonp=callback'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertNotIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

        response = self.app.get('/auctions/{}/awards/{}/documents?opt_pretty=1'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('{\n    "', response.body)
        self.assertNotIn('callback({', response.body)

        response = self.app.get('/auctions/{}/awards/{}/documents?opt_jsonp=callback&opt_pretty=1'.format(auction['id'], award['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('{\n    "', response.body)
        self.assertIn('callback({', response.body)

    def test_listing_changes(self):

        auction = self.create_auction()

        data = self.db[auction['id']]

        award = data['awards'][0]
        award_documents = award['documents']

        for i in range(3):
            document = deepcopy(test_document)
            document['dateModified'] = get_now().isoformat()
            document['id'] = uuid4().hex
            award_documents.append(document)

        self.db.save(data)

        ids = ','.join([i['id'] for i in award_documents])

        response = self.app.get('/auctions/{}/awards/{}/documents'.format(auction['id'], award['id']))
        self.assertTrue(ids.startswith(','.join([i['id'] for i in response.json['data']])))

        self.assertEqual(response.status, '200 OK')
        self.assertEqual(len(response.json['data']), len(award_documents))
        self.assertEqual(set([i['id'] for i in response.json['data']]), set([i['id'] for i in award_documents]))
        self.assertEqual(set([i['dateModified'] for i in response.json['data']]), set([i['dateModified'] for i in award_documents]))
        self.assertEqual([i['dateModified'] for i in response.json['data']], sorted([i['dateModified'] for i in award_documents]))

    def test_get_award_document(self):
        auction = self.create_auction()

        award = auction['awards'][0]
        award_document = award['documents'][0]

        response = self.app.get('/auctions/{}/awards/{}/documents/{}'.format(auction['id'], award['id'], award_document['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertDictEqual(response.json['data'], award_document)

        response = self.app.get('/auctions/{}/awards/{}/documents/{}?opt_jsonp=callback'.format(auction['id'], award['id'],award_document['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/javascript')
        self.assertIn('callback({"data": {"', response.body)

        response = self.app.get('/auctions/{}/awards/{}/documents/{}?opt_pretty=1'.format(auction['id'], award['id'], award_document['id']))
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('{\n    "data": {\n        "', response.body)

    def test_award_document_not_found(self):
        auction = self.create_auction()

        response = self.app.get('/auctions/{}/awards/{}/documents/some_id'.format(auction['id'], auction['awards'][0]['id']), status=404)
        self.assertEqual(response.status, '404 Not Found')
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.json['status'], 'error')
        self.assertEqual(response.json['errors'], [
            {u'description': u'Not Found', u'location': u'url', u'name': u'document_id'}
        ])



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(AuctionResourceTest))
    suite.addTest(unittest.makeSuite(AuctionAwardResourceTest))
    suite.addTest(unittest.makeSuite(AuctionAwardDocumentResourceTest))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
