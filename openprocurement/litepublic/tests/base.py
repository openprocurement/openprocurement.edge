# -*- coding: utf-8 -*-
import unittest
import webtest
import os
from copy import deepcopy
from datetime import datetime, timedelta
from openprocurement.api.models import get_now
from uuid import uuid4


from openprocurement.api.models import SANDBOX_MODE
from openprocurement.api.utils import VERSION
from openprocurement.api.design import sync_design
from openprocurement.api.tests.base import BaseWebTest, test_tender_data, test_bids, test_organization, test_lots

try:
    from openprocurement.contracting.api.tests.base import test_contract_data
except ImportError:
    test_contract_data = None

test_award = {'suppliers': [test_organization], 'status': 'pending', 'bid_id': ''}

test_complaint = {'title': 'complaint title', 'description': 'complaint description', 'author': test_organization}

test_document = {
    'title': 'укр.doc',
    'url': 'ds.op/ukr',
    'hash': 'md5:' + '0' * 32,
    'format': 'application/msword',
}

test_auction_data = test_tender_data
test_auction_bids = deepcopy(test_bids)
for bid in test_auction_bids:
    bid['value']['amount'] = bid['value']['amount'] * bid['value']['amount']


class TenderBaseWebTest(BaseWebTest):
    initial_data = test_tender_data
    initial_lots = test_lots
    initial_bids = test_bids
    initial_award = test_award
    initial_document = test_document
    initial_award_complaint = test_complaint
    initial_award_complaint_document = test_document

    relative_to = os.path.dirname(__file__)

    def create_tender(self, initial_data=initial_data):
        data = deepcopy(initial_data)
        data['_id'] = data['id'] = uuid4().hex
        data['status'] = 'active'
        data['doc_type'] = "Tender"
        data['dateModified'] = get_now().isoformat()
        data['tenderID'] = "UA-X"
        if self.initial_lots:
            lots = []
            for i in self.initial_lots:
                lot = deepcopy(i)
                lot['id'] = uuid4().hex
                lots.append(lot)
            data['lots'] = self.initial_lots = lots
            for i, item in enumerate(data['items']):
                item['relatedLot'] = lots[i % len(lots)]['id']
        if self.initial_bids:
            bids = []
            for i in self.initial_bids:
                if self.initial_lots:
                    i = i.copy()
                    i['id'] = uuid4().hex
                    value = i.pop('value')
                    i['lotValues'] = [
                        {
                            'value': value,
                            'relatedLot': l['id'],
                        }
                        for l in self.initial_lots
                    ]
                bids.append(i)
            data['bids'] = bids
            if self.initial_award:
                award = deepcopy(self.initial_award)
                award['id'] = uuid4().hex
                award['bid_id'] = bids[0]['id']
                award['date'] = get_now().isoformat()
                data['awards'] = [award]
                if self.initial_award_complaint:
                    award_complaint = deepcopy(self.initial_award_complaint)
                    award_complaint['id'] = uuid4().hex
                    award_complaint['date'] = get_now().isoformat()
                    data['awards'][0]['complaints'] = [award_complaint]
                    if self.initial_award_complaint_document:
                        award_complaint_document = deepcopy(self.initial_award_complaint_document)
                        award_complaint_document['id'] = uuid4().hex
                        award_complaint_document['dateModified'] = get_now().isoformat()
                        data['awards'][0]['complaints'][0]['documents'] = [award_complaint_document]
        if self.initial_document:
            document = deepcopy(self.initial_document)
            document['id'] = uuid4().hex
            document['dateModified'] = get_now().isoformat()
            data['documents'] = [document]
        self.db.save(data)
        data = self.db[data['id']]
        del data['_id']
        del data['_rev']
        del data['doc_type']
        return data


class AuctionBaseWebTest(BaseWebTest):
    initial_data = test_auction_data
    initial_bids = test_auction_bids
    initial_award = test_award
    initial_award_document = test_document
    initial_document = test_document

    relative_to = os.path.dirname(__file__)

    def create_auction(self, initial_data=initial_data):
        data = deepcopy(initial_data)
        data['_id'] = data['id'] = uuid4().hex
        data['status'] = 'active'
        data['doc_type'] = "Auction"
        data['dateModified'] = get_now().isoformat()
        data['auctionID'] = "UA-EA-X"

        if self.initial_bids:
            data['bids'] = deepcopy(self.initial_bids)
            data['bids'][0]['id'] = uuid4().hex
            if self.initial_award:
                award = deepcopy(self.initial_award)
                award['id'] = uuid4().hex
                award['bid_id'] = data['bids'][0]['id']
                award['date'] = get_now().isoformat()
                data['awards'] = [award]
                if self.initial_award_document:
                    document = deepcopy(self.initial_award_document)
                    document['id'] = uuid4().hex
                    document['dateModified'] = get_now().isoformat()
                    data['awards'][0]['documents'] = [document]
        if self.initial_document:
            document = deepcopy(self.initial_document)
            document['id'] = uuid4().hex
            document['dateModified'] = get_now().isoformat()
            data['documents'] = [document]
        self.db.save(data)
        data = self.db[data['id']]
        del data['_id']
        del data['_rev']
        del data['doc_type']
        return data


class ContractBaseWebTest(BaseWebTest):
    initial_data = test_auction_data
    initial_document = test_document

    relative_to = os.path.dirname(__file__)

    def create_contract(self, initial_data=initial_data):
        data = deepcopy(initial_data)
        data['_id'] = data['id'] = uuid4().hex
        data['status'] = 'active'
        data['doc_type'] = "Contract"
        data['dateModified'] = get_now().isoformat()
        data['auctionID'] = "UA-X"
        if self.initial_document:
            document = deepcopy(self.initial_document)
            document['id'] = uuid4().hex
            document['dateModified'] = get_now().isoformat()
            data['documents'] = [document]
        self.db.save(data)
        data = self.db[data['id']]
        del data['_id']
        del data['_rev']
        del data['doc_type']
        return data
