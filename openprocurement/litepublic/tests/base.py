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
from openprocurement.api.tests.base import BaseWebTest

try:
    from openprocurement.contracting.api.tests.base import test_contract_data
except ImportError:
    test_contract_data = None

now = datetime.now()
test_organization = {
    "name": "Державне управління справами",
    "identifier": {
        "scheme": "UA-EDR",
        "id": "00037256",
        "uri": "http://www.dus.gov.ua/"
    },
    "address": {
        "countryName": "Україна",
        "postalCode": "01220",
        "region": "м. Київ",
        "locality": "м. Київ",
        "streetAddress": "вул. Банкова, 11, корпус 1"
    },
    "contactPoint": {
        "name": "Державне управління справами",
        "telephone": "0440000000"
    }
}
test_procuringEntity = test_organization.copy()
test_tender_data = {
    "title": "футляри до державних нагород",
    "procuringEntity": test_procuringEntity,
    "value": {
        "amount": 500,
        "currency": "UAH"
    },
    "minimalStep": {
        "amount": 35,
        "currency": "UAH"
    },
    "items": [
        {
            "description": "футляри до державних нагород",
            "classification": {
                "scheme": "CPV",
                "id": "44617100-9",
                "description": "Cartons"
            },
            "additionalClassifications": [
                {
                    "scheme": "ДКПП",
                    "id": "17.21.1",
                    "description": "папір і картон гофровані, паперова й картонна тара"
                }
            ],
            "unit": {
                "name": "item",
                "code": "44617100-9"
            },
            "quantity": 5,
            "deliveryDate": {
                "startDate": (now + timedelta(days=2)).isoformat(),
                "endDate": (now + timedelta(days=5)).isoformat()
            },
            "deliveryAddress": {
                "countryName": "Україна",
                "postalCode": "79000",
                "region": "м. Київ",
                "locality": "м. Київ",
                "streetAddress": "вул. Банкова 1"
            }
        }
    ],
    "enquiryPeriod": {
        "endDate": (now + timedelta(days=7)).isoformat()
    },
    "tenderPeriod": {
        "endDate": (now + timedelta(days=14)).isoformat()
    },
    "procurementMethodType": "belowThreshold",
}
if SANDBOX_MODE:
    test_tender_data['procurementMethodDetails'] = 'quick, accelerator=1440'
test_features_tender_data = test_tender_data.copy()
test_features_item = test_features_tender_data['items'][0].copy()
test_features_item['id'] = "1"
test_features_tender_data['items'] = [test_features_item]
test_features_tender_data["features"] = [
    {
        "code": "OCDS-123454-AIR-INTAKE",
        "featureOf": "item",
        "relatedItem": "1",
        "title": "Потужність всмоктування",
        "title_en": "Air Intake",
        "description": "Ефективна потужність всмоктування пилососа, в ватах (аероватах)",
        "enum": [
            {
                "value": 0.1,
                "title": "До 1000 Вт"
            },
            {
                "value": 0.15,
                "title": "Більше 1000 Вт"
            }
        ]
    },
    {
        "code": "OCDS-123454-YEARS",
        "featureOf": "tenderer",
        "title": "Років на ринку",
        "title_en": "Years trading",
        "description": "Кількість років, які організація учасник працює на ринку",
        "enum": [
            {
                "value": 0.05,
                "title": "До 3 років"
            },
            {
                "value": 0.1,
                "title": "Більше 3 років, менше 5 років"
            },
            {
                "value": 0.15,
                "title": "Більше 5 років"
            }
        ]
    }
]
test_bids = [
    {
        "tenderers": [
            test_organization
        ],
        "value": {
            "amount": 469,
            "currency": "UAH",
            "valueAddedTaxIncluded": True
        }
    },
    {
        "tenderers": [
            test_organization
        ],
        "value": {
            "amount": 479,
            "currency": "UAH",
            "valueAddedTaxIncluded": True
        }
    }
]
test_lots = [
    {
        'title': 'lot title',
        'description': 'lot description',
        'value': test_tender_data['value'],
        'minimalStep': test_tender_data['minimalStep'],
    }
]
test_features = [
    {
        "code": "code_item",
        "featureOf": "item",
        "relatedItem": "1",
        "title": "item feature",
        "enum": [
            {
                "value": 0.01,
                "title": "good"
            },
            {
                "value": 0.02,
                "title": "best"
            }
        ]
    },
    {
        "code": "code_tenderer",
        "featureOf": "tenderer",
        "title": "tenderer feature",
        "enum": [
            {
                "value": 0.01,
                "title": "good"
            },
            {
                "value": 0.02,
                "title": "best"
            }
        ]
    }
]

test_award = {'suppliers': [test_organization], 'status': 'pending', 'bid_id': ''}

test_complaint = {'title': 'complaint title', 'description': 'complaint description', 'author': test_organization}

test_document = {
    'title': 'укр.doc',
    'url': 'ds.op/ukr',
    'hash': 'md5:' + '0' * 32,
    'format': 'application/msword',
}

test_auction_data = deepcopy(test_tender_data)
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
