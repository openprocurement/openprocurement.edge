# -*- coding: utf-8 -*-
from cornice.resource import resource
from functools import partial
from openprocurement.litepublic.traversal import factory
from pyramid.exceptions import URLDecodeError
from pyramid.compat import decode_path_info
from munch import munchify
from openprocurement.api.utils import error_handler


opresource = partial(resource, error_handler=error_handler, factory=factory)


def extract_tender_adapter(request, tender_id):
    db = request.registry.db
    doc = db.get(tender_id)
    if doc is None or doc.get('doc_type') != 'Tender':
        request.errors.add('url', 'tender_id', 'Not Found')
        request.errors.status = 404
        raise error_handler(request.errors)

    return munchify(doc)


def extract_tender(request):
    try:
        # empty if mounted under a path in mod_wsgi, for example
        path = decode_path_info(request.environ['PATH_INFO'] or '/')
    except KeyError:
        path = '/'
    except UnicodeDecodeError as e:
        raise URLDecodeError(e.encoding, e.object, e.start, e.end, e.reason)

    tender_id = ""
    # extract tender id
    parts = path.split('/')
    if len(parts) < 4 or parts[3] != 'tenders':
        return

    tender_id = parts[4]
    return extract_tender_adapter(request, tender_id)
