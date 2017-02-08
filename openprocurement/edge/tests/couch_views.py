# -*- coding: utf-8 -*-
import os
import unittest
import uuid
from requests import Session
from couchdb import Server
import simplejson as json
from openprocurement.edge.utils import push_views


class TestCouchViews(unittest.TestCase):

    relative_to = os.path.dirname(__file__)

    @classmethod
    def setUpClass(cls):
        cls.couchdb_url = 'http://127.0.0.1:5984'
        server = Server(cls.couchdb_url)
        if 'test_db' in server:
            cls.db = server['test_db']
        else:
            cls.db = server.create('test_db')
        array_path = os.path.dirname(os.path.abspath(__file__)).split('/')
        app_path = ""
        for p in array_path[:-1]:
            app_path += p + '/'
        app_path += 'couch_views'
        couchdb_url = cls.couchdb_url + '/test_db'
        for resource in ('/tenders', '/plans', '/contracts', '/auctions'):
            push_views(couchapp_path=app_path + resource,
                       couch_url=couchdb_url)
        cls.app = Session()
        cls.tender_id = uuid.uuid4().hex
        cls.plan_id = uuid.uuid4().hex
        cls.contract_id = uuid.uuid4().hex
        cls.auction_id = uuid.uuid4().hex
        cls.tenders_path = [
            {
                'url': '/test_db/_design/tenders/_show/show/{}'.format(cls.tender_id),
                'template': 'files/tid.json'
            },
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=*'.format(cls.tender_id),
             'template': 'files/awards.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb'.format(cls.tender_id), 'template': 'files/f0d5fd00743b46668f6b589496ad73eb.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=*'.format(cls.tender_id), 'template': 'files/awards_complaints.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=c2a9a67e05314669a3c578043cfa91ba'.format(cls.tender_id), 'template': 'files/c2a9a67e05314669a3c578043cfa91ba.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=c2a9a67e05314669a3c578043cfa91ba&document_id=*'.format(cls.tender_id), 'template': 'files/awards_complaints_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=c2a9a67e05314669a3c578043cfa91ba&document_id=db8c4cd39a81472087c4f44880f91a4f'.format(cls.tender_id), 'template': 'files/db8c4cd39a81472087c4f44880f91a4f.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&document_id=*'.format(cls.tender_id), 'template': 'files/awards_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&document_id=889ba5a3d0f345939213cb0cd4bbd5cc'.format(cls.tender_id), 'template': 'files/889ba5a3d0f345939213cb0cd4bbd5cc.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=*'.format(cls.tender_id), 'template': 'files/bids.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c'.format(cls.tender_id), 'template': 'files/d391b38ce13b44fe88c832bb64ec7f3c.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&document_id=*'.format(cls.tender_id), 'template': 'files/bids_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&document_id=fb5e261dd3cb48489d5f6735663d968e'.format(cls.tender_id), 'template': 'files/fb5e261dd3cb48489d5f6735663d968e.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&eligibility_document=*'.format(cls.tender_id), 'template': 'files/bids_eligibility_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&eligibility_document=bad95fc7808448898f2a7555c9b17c0b'.format(cls.tender_id), 'template': 'files/bad95fc7808448898f2a7555c9b17c0b.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&financial_document=*'.format(cls.tender_id), 'template': 'files/bids_financial_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&financial_document=4c98d31f48704b44b17cdce8025702ca'.format(cls.tender_id), 'template': 'files/4c98d31f48704b44b17cdce8025702ca.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&qualification_document=*'.format(cls.tender_id), 'template': 'files/bids_qualification_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?bid_id=d391b38ce13b44fe88c832bb64ec7f3c&qualification_document=1c9548d090f94f24837c7b39c041bc78'.format(cls.tender_id), 'template': 'files/1c9548d090f94f24837c7b39c041bc78.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?cancellation_id=*'.format(cls.tender_id), 'template': 'files/cancellations.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?cancellation_id=e6bd49d00bde4847b84e936489cf5df5'.format(cls.tender_id), 'template': 'files/e6bd49d00bde4847b84e936489cf5df5.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?cancellation_id=e6bd49d00bde4847b84e936489cf5df5&document_id=*'.format(cls.tender_id), 'template': 'files/cancellations_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?cancellation_id=e6bd49d00bde4847b84e936489cf5df5&document_id=e5d1cfd73fba43cc8296e7ac827eb269'.format(cls.tender_id), 'template': 'files/e5d1cfd73fba43cc8296e7ac827eb269.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?complaint_id=*'.format(cls.tender_id), 'template': 'files/complaints.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?complaint_id=c495b3ea6fb04ed6ba438cad2e91d817'.format(cls.tender_id), 'template': 'files/c495b3ea6fb04ed6ba438cad2e91d817.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?complaint_id=c495b3ea6fb04ed6ba438cad2e91d817&document_id=*'.format(cls.tender_id), 'template': 'files/complaints_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?complaint_id=c495b3ea6fb04ed6ba438cad2e91d817&document_id=a8779280923f43888b428ecbc20fc0af'.format(cls.tender_id), 'template': 'files/a8779280923f43888b428ecbc20fc0af.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?contract_id=*'.format(cls.tender_id), 'template': 'files/contracts.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?contract_id=d5d24e1c74fd4c7399e0ea44aaa8d2ad'.format(cls.tender_id), 'template': 'files/d5d24e1c74fd4c7399e0ea44aaa8d2ad.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?contract_id=d5d24e1c74fd4c7399e0ea44aaa8d2ad&document_id=*'.format(cls.tender_id), 'template': 'files/contracts_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?contract_id=d5d24e1c74fd4c7399e0ea44aaa8d2ad&document_id=ff45158cca2e46209dd080674dea25ae'.format(cls.tender_id), 'template': 'files/ff45158cca2e46209dd080674dea25ae.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?document_id=*'.format(cls.tender_id), 'template': 'files/documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?document_id=18550fb4c68b49b9a1bde355779f314c'.format(cls.tender_id), 'template': 'files/18550fb4c68b49b9a1bde355779f314c.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?lot_id=*'.format(cls.tender_id), 'template': 'files/lots.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?lot_id=422eab9551d84b7d9469ed4f1640b2bc'.format(cls.tender_id), 'template': 'files/422eab9551d84b7d9469ed4f1640b2bc.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=*'.format(cls.tender_id), 'template': 'files/qualifications.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc'.format(cls.tender_id), 'template': 'files/efa0d365d009477b840d0c0422489abc.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc&document_id=*'.format(cls.tender_id), 'template': 'files/qualifications_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc&document_id=a794e1b59c9242718196a575ee1526d5'.format(cls.tender_id), 'template': 'files/a794e1b59c9242718196a575ee1526d5.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc&q_complaint_id=*'.format(cls.tender_id), 'template': 'files/qualifications_complaints.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc&q_complaint_id=911f5d9e924f4408acf0292848085ca3'.format(cls.tender_id), 'template': 'files/911f5d9e924f4408acf0292848085ca3.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc&q_complaint_id=911f5d9e924f4408acf0292848085ca3&document_id=*'.format(cls.tender_id), 'template': 'files/qualifications_complaints_documents.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?qualification_id=efa0d365d009477b840d0c0422489abc&q_complaint_id=911f5d9e924f4408acf0292848085ca3&document_id=c623089caea44d798a6ce61240e42915'.format(cls.tender_id), 'template': 'files/c623089caea44d798a6ce61240e42915.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?question_id=*'.format(cls.tender_id), 'template': 'files/questions.json'},
            {'url': '/test_db/_design/tenders/_show/show/{}?question_id=92cbea1350464cbca7cd85ef8a2cdb1e'.format(cls.tender_id), 'template': 'files/92cbea1350464cbca7cd85ef8a2cdb1e.json'}
        ]
        cls.auctions_path = [
            {'url': '/test_db/_design/auctions/_show/show/{}'.format(cls.auction_id),
             'template': 'files/aid.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=*'.format(cls.auction_id),
             'template': 'files/auctions_awards.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb'.format(cls.auction_id), 'template': 'files/f0d5fd00743b46668f6b589496ad73eb.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=*'.format(cls.auction_id), 'template': 'files/awards_complaints.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=c2a9a67e05314669a3c578043cfa91ba'.format(cls.auction_id), 'template': 'files/c2a9a67e05314669a3c578043cfa91ba.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=c2a9a67e05314669a3c578043cfa91ba&document_id=*'.format(cls.auction_id), 'template': 'files/awards_complaints_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&complaint_id=c2a9a67e05314669a3c578043cfa91ba&document_id=db8c4cd39a81472087c4f44880f91a4f'.format(cls.auction_id), 'template': 'files/db8c4cd39a81472087c4f44880f91a4f.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&document_id=*'.format(cls.auction_id), 'template': 'files/awards_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?award_id=f0d5fd00743b46668f6b589496ad73eb&document_id=889ba5a3d0f345939213cb0cd4bbd5cc'.format(cls.auction_id), 'template': 'files/889ba5a3d0f345939213cb0cd4bbd5cc.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?bid_id=*'.format(cls.auction_id), 'template': 'files/auctions_bids.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?bid_id=afefa29c839b4fa6be5eab1a949b13da'.format(cls.auction_id), 'template': 'files/afefa29c839b4fa6be5eab1a949b13da.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?bid_id=afefa29c839b4fa6be5eab1a949b13da&document_id=*'.format(cls.auction_id), 'template': 'files/auctions_bids_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?bid_id=afefa29c839b4fa6be5eab1a949b13da&document_id=5617f2d1a30d48f99f4b8c0aceeddbb0'.format(cls.auction_id), 'template': 'files/5617f2d1a30d48f99f4b8c0aceeddbb0.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?cancellation_id=*'.format(cls.auction_id), 'template': 'files/auctions_cancellations.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?cancellation_id=c537f609bd024a73aec071db7ec0733f'.format(cls.auction_id), 'template': 'files/c537f609bd024a73aec071db7ec0733f.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?cancellation_id=c537f609bd024a73aec071db7ec0733f&document_id=*'.format(cls.auction_id), 'template': 'files/auctions_cancellations_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?cancellation_id=c537f609bd024a73aec071db7ec0733f&document_id=793b7fc243f4427cb3bd06063396783d'.format(cls.auction_id), 'template': 'files/793b7fc243f4427cb3bd06063396783d.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?complaint_id=*'.format(cls.auction_id), 'template': 'files/auctions_complaints.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?complaint_id=e61ccbae10fe48fb9e2bc69742ccd0b4'.format(cls.auction_id), 'template': 'files/e61ccbae10fe48fb9e2bc69742ccd0b4.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?complaint_id=e61ccbae10fe48fb9e2bc69742ccd0b4&document_id=*'.format(cls.auction_id), 'template': 'files/auctions_complaints_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?complaint_id=e61ccbae10fe48fb9e2bc69742ccd0b4&document_id=b74879c12361469f909eb9b0ac348a2e'.format(cls.auction_id), 'template': 'files/b74879c12361469f909eb9b0ac348a2e.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?contract_id=*'.format(cls.auction_id), 'template': 'files/auctions_contracts.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?contract_id=df4ec5a5ee8844d68338093048c708a8'.format(cls.auction_id), 'template': 'files/df4ec5a5ee8844d68338093048c708a8.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?contract_id=df4ec5a5ee8844d68338093048c708a8&document_id=*'.format(cls.auction_id), 'template': 'files/auctions_contracts_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?contract_id=df4ec5a5ee8844d68338093048c708a8&document_id=77e7cf19a24b49c3900cb90c7617ed50'.format(cls.auction_id), 'template': 'files/77e7cf19a24b49c3900cb90c7617ed50.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?document_id=*'.format(cls.auction_id), 'template': 'files/auctions_documents.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?document_id=1a725272c7544e7298977042057b7b47'.format(cls.auction_id), 'template': 'files/1a725272c7544e7298977042057b7b47.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?lot_id=*'.format(cls.auction_id), 'template': 'files/auctions_lots.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?lot_id=3efb93d14cec4a36a71ffbea04b97399'.format(cls.auction_id), 'template': 'files/3efb93d14cec4a36a71ffbea04b97399.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?question_id=*'.format(cls.auction_id), 'template': 'files/auctions_questions.json'},
            {'url': '/test_db/_design/auctions/_show/show/{}?question_id=61adb70f5fe047feb48919faa847911d'.format(cls.auction_id), 'template': 'files/61adb70f5fe047feb48919faa847911d.json'}
        ]
        cls.plans_path = [
            {'url': '/test_db/_design/plans/_show/show/{}'.format(cls.plan_id),
            'template': 'files/pid.json'},
            {'url': '/test_db/_design/plans/_show/show/{}?document_id=*'.format(cls.plan_id),
            'template': 'files/plans_documents.json'},
            {'url': '/test_db/_design/plans/_show/show/{}?document_id=dbc6246ce0914eb4bda3b457bd562eac'.format(cls.plan_id), 'template': 'files/dbc6246ce0914eb4bda3b457bd562eac.json'},
        ]
        cls.contracts_path = [
            {'url': '/test_db/_design/contracts/_show/show/{}'.format(cls.contract_id),
             'template': 'files/cid.json'},
            {'url': '/test_db/_design/contracts/_show/show/{}?document_id=*'.format(cls.contract_id),
             'template': 'files/c_contracts_documents.json'},
            {'url': '/test_db/_design/contracts/_show/show/{}?document_id=d130ea7f3d89433fa8e4d6d3011d7028'.format(cls.contract_id), 'template': 'files/d130ea7f3d89433fa8e4d6d3011d7028.json'},
        ]

    def reset_document_url(self, bids_edge=None, bids_couch=None):
        if bids_edge is None or bids_couch is None:
            return
        for bid in bids_edge:
            if 'documents' in bid:
                for doc in bid['documents']:
                    if doc['confidentiality'] == 'buyerOnly':
                        doc['url'] = ''
        for bid in bids_couch:
            if 'documents' in bid:
                for doc in bid['documents']:
                    if doc['confidentiality'] == 'buyerOnly':
                        doc['url'] = ''

    def check_resource_views(self, list_path, base_doc, doc_id):

        absolute_path = os.path.dirname(__file__) + '/files/' + base_doc
        json_doc = open(absolute_path, 'r').read()
        doc_dict = json.loads(json_doc)
        doc_dict['id'] = doc_id
        doc_dict['_id'] = doc_id
        self.db.save(doc_dict)

        for u in list_path:
            resp = self.app.get(self.couchdb_url + u['url'])
            template_path = os.path.dirname(__file__) + '/' + u['template']
            with open(template_path, 'r') as template:
                template_dict = json.loads(template.read())
                if (u['template'] == 'files/tid.json' or
                        u['template'] == 'files/aid.json' or
                        u['template'] == 'files/pid.json' or
                        u['template'] == 'files/cid.json'):
                    template_dict['data']['id'] = doc_id
                print u['url']
                self.assertEqual(resp.json(), template_dict)

        doc = self.db.get(doc_id)
        self.db.delete(doc)

    def test_auction_views(self):
        self.check_resource_views(self.auctions_path,
                                  'test_auction.json',
                                  self.auction_id)

    def test_contract_views(self):
        self.check_resource_views(self.contracts_path,
                                  'test_contract.json',
                                  self.contract_id)

    def test_plan_views(self):
        self.check_resource_views(self.plans_path,
                                  'test_plan.json',
                                  self.plan_id)

    def test_tender_views(self):
        self.check_resource_views(self.tenders_path,
                                  'test_tender.json',
                                  self.tender_id)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCouchViews))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
