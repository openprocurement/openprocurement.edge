# -*- coding: utf-8 -*-
import unittest
import webtest
import os
from datetime import datetime, timedelta
from copy import deepcopy
from openprocurement.edge.utils import get_now
from openprocurement.edge.design import sync_design
from openprocurement.edge.utils import push_views
from uuid import uuid4

now = datetime.now()

test_organization = {
    "name": u"Державне управління справами",
    "identifier": {
        "scheme": u"UA-EDR",
        "id": u"00037256",
        "uri": u"http://www.dus.gov.ua/"
    },
    "address": {
        "countryName": u"Україна",
        "postalCode": u"01220",
        "region": u"м. Київ",
        "locality": u"м. Київ",
        "streetAddress": u"вул. Банкова, 11, корпус 1"
    },
    "contactPoint": {
        "name": u"Державне управління справами",
        "telephone": u"0440000000"
    }
}


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

test_procuringEntity = test_organization.copy()
test_procuringEntity["kind"] = "general"

test_tender_data = {
    "title": u"футляри до державних нагород",
    "procuringEntity": test_procuringEntity,
    "value": {
        "amount": 500,
        "currency": u"UAH"
    },
    "minimalStep": {
        "amount": 35,
        "currency": u"UAH"
    },
    "items": [
        {
            "description": u"футляри до державних нагород",
            "classification": {
                "scheme": u"CPV",
                "id": u"44617100-9",
                "description": u"Cartons"
            },
            "additionalClassifications": [
                {
                    "scheme": u"ДКПП",
                    "id": u"17.21.1",
                    "description": u"папір і картон гофровані, паперова й картонна тара"
                }
            ],
            "unit": {
                "name": u"item",
                "code": u"44617100-9"
            },
            "quantity": 5,
            "deliveryDate": {
                "startDate": (now + timedelta(days=2)).isoformat(),
                "endDate": (now + timedelta(days=5)).isoformat()
            },
            "deliveryAddress": {
                "countryName": u"Україна",
                "postalCode": "79000",
                "region": u"м. Київ",
                "locality": u"м. Київ",
                "streetAddress": u"вул. Банкова 1"
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

test_lots = [
    {
        'title': 'lot title',
        'description': 'lot description',
        'value': test_tender_data['value'],
        'minimalStep': test_tender_data['minimalStep'],
    }
]

test_contract_data = {
    u"items": [
        {
        u"description": u"футляри до державних нагород",
        u"classification": {
            u"scheme": u"CPV",
            u"description": u"Cartons",
            u"id": u"44617100-9"
        },
        u"additionalClassifications": [
            {
                u"scheme": u"ДКПП",
                u"id": u"17.21.1",
                u"description": u"папір і картон гофровані, паперова й картонна тара"
            }
        ],
        u"deliveryAddress": {
                        u"postalCode": u"79000",
                        u"countryName": u"Україна",
                        u"streetAddress": u"вул. Банкова 1",
                        u"region": u"м. Київ",
                        u"locality": u"м. Київ"
                    },
        u"deliveryDate": {
                        u"startDate": u"2016-03-20T18:47:47.136678+02:00",
                        u"endDate": u"2016-03-23T18:47:47.136678+02:00"
                    },
        u"id": u"c6c6e8ed4b1542e4bf13d3f98ec5ab59",
        u"unit": {
            u"code": u"44617100-9",
            u"name": u"item"
        },
        u"quantity": 5
        }
    ],
    u"procuringEntity": {
        u"name": u"Державне управління справами",
        u"identifier": {
            u"scheme": u"UA-EDR",
            u"id": u"00037256",
            u"uri": u"http://www.dus.gov.ua/"
        },
        u"address": {
            u"countryName": u"Україна",
            u"postalCode": u"01220",
            u"region": u"м. Київ",
            u"locality": u"м. Київ",
            u"streetAddress": u"вул. Банкова, 11, корпус 1"
        },
        u"contactPoint": {
            u"name": u"Державне управління справами",
            u"telephone": u"0440000000"
        }
    },
    u"suppliers": [
        {
        u"contactPoint": {
            u"email": u"aagt@gmail.com",
            u"telephone": u"+380 (322) 91-69-30",
            u"name": u"Андрій Олексюк"
        },
        u"identifier": {
            u"scheme": u"UA-EDR",
            u"id": u"00137226",
            u"uri": u"http://www.sc.gov.ua/"
        },
        u"name": u"ДКП «Книга»",
        u"address": {
                    u"postalCode": u"79013",
                    u"countryName": u"Україна",
                    u"streetAddress": u"вул. Островського, 34",
                    u"region": u"м. Львів",
                    u"locality": u"м. Львів"
                    }
        }
    ],
    u"contractNumber": u"contract #13111",
    u"period": {
                u"startDate": u"2016-03-18T18:47:47.155143+02:00",
                u"endDate": u"2017-03-18T18:47:47.155143+02:00"
            },
    u"value": {
        u"currency": u"UAH",
        u"amount": 238.0,
        u"valueAddedTaxIncluded": True
        },
    u"dateSigned": get_now().isoformat(),
    u"awardID": u"8481d7eb01694c25b18658036c236c5d",
    u"id": uuid4().hex,
    u"contractID": u"UA-2016-03-18-000001-1",
    u"tender_id": uuid4().hex,
    u"tender_token": uuid4().hex,
    u"owner": u"broker"
}

test_plan_data =  {
        "tender": {
            "procurementMethod": u"open",
            "procurementMethodType": u"belowThreshold",
            "tenderPeriod": {
                "startDate": (now + timedelta(days=7)).isoformat()
            }
        },
        "items": [
            {
                "deliveryDate": {
                    "endDate": (now + timedelta(days=15)).isoformat()
                },
                "additionalClassifications": [
                    {
                        "scheme": u"ДКПП",
                        "id": u"01.11.92",
                        "description": u"Насіння гірчиці"
                    }
                ],
                "unit": {
                    "code": u"KGM",
                    "name": u"кг"
                },
                "classification": {
                    "scheme": u"CPV",
                    "description": u"Mustard seeds",
                    "id": u"03111600-8"
                },
                "quantity": 1000,
                "description": u"Насіння гірчиці"
            },
            {
                "deliveryDate": {
                    "endDate": (now + timedelta(days=16)).isoformat()
                },
                "additionalClassifications": [
                    {
                        "scheme": u"ДКПП",
                        "id": u"01.11.95",
                        "description": u"Насіння соняшнику"
                    }
                ],
                "unit": {
                    "code": u"KGM",
                    "name": u"кг"
                },
                "classification": {
                    "scheme": u"CPV",
                    "description": u"Sunflower seeds",
                    "id": u"03111300-5"
                },
                "quantity": 2000,
                "description": u"Насіння соняшнику"
            },
            {
                "deliveryDate": {
                    "endDate": (now + timedelta(days=17)).isoformat()
                },
                "additionalClassifications": [
                    {
                        "scheme": u"ДКПП",
                        "id": u"01.11.84",
                        "description": u"Насіння бавовнику"
                    }
                ],
                "unit": {
                    "code": u"KGM",
                    "name": u"кг"
                },
                "classification": {
                    "scheme": u"CPV",
                    "description": u"Cotton seeds",
                    "id": u"03111400-6"
                },
                "quantity": 3000,
                "description": u"Насіння бавовнику"
            }
        ],
        "classification": {
            "scheme": u"CPV",
            "description": u"Seeds",
            "id": u"03111000-2"
        },
        "additionalClassifications": [
            {
                "scheme": u"КЕКВ",
                "id": u"1",
                "description": u"-"
            }
        ],
        "procuringEntity": {
            "identifier": {
                "scheme": u"UA-EDR",
                "id": u"111983",
                "legalName": u"ДП Державне Управління Справами"
            },
            "name": u"ДУС"
        },
        "budget": {
            "project": {
                "name": u"proj_name",
                "id": u"123"
            },
            "amount": 10000,
            "amountNet": 12222,
            "currency": u"UAH",
            "id": u"12303111000-2",
            "description": u"budget_description"
        }
    }


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

VERSION = '0'
ROUTE_PREFIX = '/api/{}'.format(VERSION)

class PrefixedRequestClass(webtest.app.TestRequest):

    @classmethod
    def blank(cls, path, *args, **kwargs):
        path = '/api/%s%s' % (VERSION, path)
        return webtest.app.TestRequest.blank(path, *args, **kwargs)



class BaseWebTest(unittest.TestCase):

    """Base Web Test to test openprocurement.api.
    It setups the database before each test and delete it after.
    """

    relative_to = os.path.dirname(__file__)

    @classmethod
    def setUpClass(cls):
        for _ in range(10):
            try:
                cls.app = webtest.TestApp("config:tests.ini", relative_to=cls.relative_to)
            except:
                pass
            else:
                break
        else:
            cls.app = webtest.TestApp("config:tests.ini", relative_to=cls.relative_to)
        cls.app.RequestClass = PrefixedRequestClass
        cls.couchdb_server = cls.app.app.registry.couchdb_server
        cls.db = cls.app.app.registry.db
        cls.db_name = cls.db.name

    @classmethod
    def tearDownClass(cls):
        try:
            cls.couchdb_server.delete(cls.db_name)
        except:
            pass

    def setUp(self):
        self.db_name += uuid4().hex
        self.couchdb_server.create(self.db_name)
        db = self.couchdb_server[self.db_name]
        #sync_design(db)
        couchapp_path = os.path.dirname(os.path.abspath(__file__))
        couchapp_path = couchapp_path.split('/')
        views_path = ''
        for i in couchapp_path[:-1]:
            views_path += i + '/'
        views_path += 'couch_views'
        couch_url = self.app.app.registry.settings['couchdb.url'] + db.name
        push_views(couchapp_path=views_path+'/tenders', couch_url=couch_url)
        push_views(couchapp_path=views_path+'/contracts', couch_url=couch_url)
        push_views(couchapp_path=views_path+'/plans', couch_url=couch_url)
        push_views(couchapp_path=views_path+'/auctions', couch_url=couch_url)
        #import pdb; pdb.set_trace()
        self.app.app.registry.db = db
        self.db = self.app.app.registry.db
        self.db_name = self.db.name
        self.app.authorization = ('Basic', ('token', ''))
        #self.app.authorization = ('Basic', ('broker', ''))

    def tearDown(self):
        self.couchdb_server.delete(self.db_name)


class TenderBaseWebTest(BaseWebTest):
    initial_data = test_tender_data
    initial_lots = test_lots
    initial_bids = test_bids
    initial_award = test_award
    initial_document = test_document
    initial_award_complaint = test_complaint
    initial_award_complaint_document = test_document

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
        data = dict(self.db[data['id']])
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
    initial_data = test_contract_data
    initial_document = test_document


    def create_contract(self, initial_data=initial_data):
        data = deepcopy(initial_data)
        data['_id'] = data['id'] = uuid4().hex
        data['status'] = 'active'
        data['doc_type'] = "Contract"
        data['dateModified'] = get_now().isoformat()
        data['contractID'] = "UA-X"
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


class PlanBaseWebTest(BaseWebTest):
    initial_data = test_plan_data
    initial_document = test_document

    def create_plan(self, initial_data=initial_data):
        data = deepcopy(initial_data)
        data['_id'] = data['id'] = uuid4().hex
        data['status'] = 'active'
        data['doc_type'] = "Plan"
        data['dateModified'] = get_now().isoformat()
        data['planID'] = "UA-X"
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
