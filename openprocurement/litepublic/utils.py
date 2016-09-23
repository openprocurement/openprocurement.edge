# -*- coding: utf-8 -*-
from cornice.resource import resource
from functools import partial
from openprocurement.litepublic.traversal import tender_factory, auction_factory, contract_factory, plan_factory
from pyramid.exceptions import URLDecodeError
from pyramid.compat import decode_path_info
from munch import munchify
from openprocurement.api.utils import error_handler

opresource = partial(resource, error_handler=error_handler, factory=tender_factory)
eaopresource = partial(resource, error_handler=error_handler, factory=auction_factory)
contractingresource = partial(resource, error_handler=error_handler, factory=contract_factory)
planningresource = partial(resource, error_handler=error_handler, factory=plan_factory)


def extract_doc_adapter(request, doc_id, doc_type):
    db = request.registry.db
    doc = db.get(doc_id)
    if doc is None or doc.get('doc_type') != doc_type:
        request.errors.add('url', '{}_id'.format(doc_type.lower()), 'Not Found')
        request.errors.status = 404
        raise error_handler(request.errors)
    return munchify(doc)


def extract_doc(request, doc_type):
    try:
        # empty if mounted under a path in mod_wsgi, for example
        path = decode_path_info(request.environ['PATH_INFO'] or '/')
    except KeyError:
        path = '/'
    except UnicodeDecodeError as e:
        raise URLDecodeError(e.encoding, e.object, e.start, e.end, e.reason)

    doc_id = ""
    # extract doc id
    parts = path.split('/')
    if len(parts) < 4 or parts[3] != '{}s'.format(doc_type.lower()):
        return

    doc_id = parts[4]
    return extract_doc_adapter(request, doc_id, doc_type)


def extract_tender(request):
    return extract_doc(request, 'Tender')


def extract_auction(request):
    return extract_doc(request, 'Auction')


def extract_contract(request):
    return extract_doc(request, 'Contract')


def extract_plan(request):
    return extract_doc(request, 'Plan')
