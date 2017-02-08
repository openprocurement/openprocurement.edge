# -*- coding: utf-8 -*-
from pyramid.security import (
    ALL_PERMISSIONS,
    Allow,
    Everyone,
)


class Root(object):
    __name__ = None
    __parent__ = None
    __acl__ = [
        (Allow, Everyone, ALL_PERMISSIONS),
    ]

    def __init__(self, request):
        self.request = request
        self.db = request.registry.db


def get_item(request, data):
    item = []
    if request.matchdict.get('items'):
        item = data
        for index, item_name in enumerate(request.matchdict['items']):
            if isinstance(item, dict):
                item = item.get(item_name, [])
            elif isinstance(item, list):
                items = [i for i in item if i['id'] == item_name]
                if len(items) > 1 and request.matchdict['items'][index - 1] == 'documents':
                    document = items.pop()
                    document['previousVersions'] = [{'url': i['url'], 'dateModified':i['dateModified']} for i in items if i.url != document.url]
                    items[0] = document
                if not items:
                    from openprocurement.edge.utils import error_handler
                    request.errors.add('url', '{}_id'.format(request.matchdict['items'][index - 1][:-1]), 'Not Found')
                    request.errors.status = 404
                    raise error_handler(request.errors)
                else:
                    item = items[0]
    return item


def tender_factory(request):
    request.validated['tender_src'] = {}
    root = Root(request)
    if not request.matchdict or not request.matchdict.get('tender_id'):
        return root
    request.validated['tender_id'] = request.matchdict['tender_id']
    tender = request.tender
    tender.__parent__ = root
    request.validated['tender'] = request.validated['db_doc'] = tender
    request.validated['tender_status'] = tender.status
    request.validated['item'] = get_item(request, tender)
    request.validated['id'] = request.matchdict['tender_id']
    return tender


def auction_factory(request):
    request.validated['auction_src'] = {}
    root = Root(request)
    if not request.matchdict or not request.matchdict.get('auction_id'):
        return root
    request.validated['auction_id'] = request.matchdict['auction_id']
    auction = request.auction
    auction.__parent__ = root
    request.validated['auction'] = request.validated['db_doc'] = auction
    request.validated['auction_status'] = auction.status
    request.validated['item'] = get_item(request, auction)
    request.validated['id'] = request.matchdict['auction_id']
    return auction


def contract_factory(request):
    request.validated['contract_src'] = {}
    root = Root(request)
    if not request.matchdict or not request.matchdict.get('contract_id'):
        return root
    request.validated['contract_id'] = request.matchdict['contract_id']
    contract = request.contract
    contract.__parent__ = root
    request.validated['contract'] = request.validated['db_doc'] = contract
    request.validated['contract_status'] = contract.status
    request.validated['item'] = get_item(request, contract)
    request.validated['id'] = request.matchdict['contract_id']
    return contract


def plan_factory(request):
    request.validated['plan_src'] = {}
    root = Root(request)
    if not request.matchdict or not request.matchdict.get('plan_id'):
        return root
    request.validated['plan_id'] = request.matchdict['plan_id']
    plan = request.plan
    plan.__parent__ = root
    request.validated['plan'] = request.validated['db_doc'] = plan
    request.validated['item'] = get_item(request, plan)
    request.validated['id'] = request.matchdict['plan_id']
    return plan
