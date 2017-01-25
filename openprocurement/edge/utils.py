# -*- coding: utf-8 -*-
import os
from binascii import hexlify, unhexlify
from cornice.resource import resource, view
from cornice.util import json_error
from couchapp.dispatch import dispatch
from datetime import datetime
from Crypto.Cipher import AES
from functools import partial
from json import dumps
from logging import getLogger
from munch import munchify
from pkg_resources import get_distribution
from pytz import timezone
from webob.multidict import NestedMultiDict

from openprocurement.edge.traversal import auction_factory, contract_factory
from openprocurement.edge.traversal import plan_factory, tender_factory

PKG = get_distribution(__package__)
LOGGER = getLogger(PKG.project_name)

TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')
VERSION = '{}.{}'.format(int(PKG.parsed_version[0]), int(PKG.parsed_version[1]) if PKG.parsed_version[1].isdigit() else 0)
ROUTE_PREFIX = '/api/{}'.format(VERSION)
SERVICE_FIELDS = ('__parent__', '_rev', '_id', 'doc_type')
json_view = partial(view, renderer='json')


class APIResource(object):

    def __init__(self, request, context):
        self.context = context
        self.request = request
        self.db = request.registry.db
        self.server_id = request.registry.server_id
        self.LOGGER = getLogger(type(self).__module__)


def get_now():
    return datetime.now(TZ)


def route_prefix(settings={}):
    return '/api/{}'.format(settings.get('api_version', VERSION))


def add_logging_context(event):
    request = event.request
    params = {
        'API_VERSION': request.registry.api_version,
        'TAGS': 'python,api',
        'USER': str(request.authenticated_userid or ''),
        'CURRENT_URL': request.url,
        'CURRENT_PATH': request.path_info,
        'REMOTE_ADDR': request.remote_addr or '',
        'USER_AGENT': request.user_agent or '',
        'REQUEST_METHOD': request.method,
        'TIMESTAMP': get_now().isoformat(),
        'REQUEST_ID': request.environ.get('REQUEST_ID', ''),
        'CLIENT_REQUEST_ID': request.headers.get('X-Client-Request-ID', ''),
    }

    request.logging_context = params


def set_logging_context(event):
    request = event.request

    params = {}
    if request.params:
        params['PARAMS'] = str(dict(request.params))
    if request.matchdict:
        for x, j in request.matchdict.items():
            params[x.upper()] = j
    if 'tender' in request.validated:
        params['TENDER_REV'] = request.validated['tender']._rev
        params['TENDERID'] = request.validated['tender'].tenderID
        params['TENDER_STATUS'] = request.validated['tender'].status
    update_logging_context(request, params)


def update_logging_context(request, params):
    if not request.__dict__.get('logging_context'):
        request.logging_context = {}

        for x, j in params.items():
                request.logging_context[x.upper()] = j


def context_unpack(request, msg, params=None):
    if params:
        update_logging_context(request, params)
    logging_context = request.logging_context
    journal_context = msg
    for key, value in logging_context.items():
        journal_context["JOURNAL_" + key] = value


def error_handler(errors, request_params=True):
    params = {
        'ERROR_STATUS': errors.status
    }
    if request_params:
        if errors.request.params:
            params['PARAMS'] = str(dict(errors.request.params))
    if errors.request.matchdict:
        for x, j in errors.request.matchdict.items():
            params[x.upper()] = j
    if 'tender' in errors.request.validated:
        params['TENDER_REV'] = errors.request.validated['tender']._rev
        params['TENDERID'] = errors.request.validated['tender'].tenderID
        params['TENDER_STATUS'] = errors.request.validated['tender'].status
    LOGGER.info('Error on processing request "{}"'.format(dumps(errors,
                                                                indent=4)),
                extra=context_unpack(errors.request,
                                     {'MESSAGE_ID': 'error_handler'}, params))
    return json_error(errors)

opresource = partial(resource, error_handler=error_handler,
                     factory=tender_factory)
eaopresource = partial(resource, error_handler=error_handler,
                       factory=auction_factory)
contractingresource = partial(resource, error_handler=error_handler,
                              factory=contract_factory)
planningresource = partial(resource, error_handler=error_handler,
                           factory=plan_factory)


def extract_doc_adapter(request, doc_id, doc_type):
    db = request.registry.db
    doc = db.get(doc_id)
    if doc is None or doc.get('doc_type') != doc_type:
        request.errors.add('url', '{}_id'.format(doc_type.lower()), 'Not Found')
        request.errors.status = 404
        raise error_handler(request.errors)
    return munchify(doc)


def clean_up_doc(doc, service_fields=SERVICE_FIELDS):
    for field in service_fields:
        if field in doc:
            del doc[field]
    return doc


def push_views(couchapp_path=None, couch_url=None):
    if couchapp_path is None or couch_url is None:
        raise Exception('Can\'t push couchapp. Please check \'couchapp_path\''
                        ' or \'couch_url\'.')
    else:
        if os.path.exists(couchapp_path):
            dispatch(['push', couchapp_path, couch_url])
        else:
            LOGGER.info('Directory {} not exist.'.format(couchapp_path))


def request_params(request):
    try:
        params = NestedMultiDict(request.GET, request.POST)
    except UnicodeDecodeError:
        request.errors.add('body', 'data', 'could not decode params')
        request.errors.status = 422
        raise error_handler(request.errors, False)
    except Exception, e:
        request.errors.add('body', str(e.__class__.__name__), str(e))
        request.errors.status = 422
        raise error_handler(request.errors, False)
    return params


def set_renderer(event):
    request = event.request
    try:
        json = request.json_body
    except ValueError:
        json = {}
        pretty = (isinstance(json, dict) and
                  json.get('options', {}).get('pretty') or
                  request.params.get('opt_pretty'))
        jsonp = request.params.get('opt_jsonp')
        if jsonp and pretty:
            request.override_renderer = 'prettyjsonp'
            return True
        if jsonp:
            request.override_renderer = 'jsonp'
            return True
        if pretty:
            request.override_renderer = 'prettyjson'
            return True


def beforerender(event):
    if (event.rendering_val and
            isinstance(event.rendering_val, dict) and
            'data' in event.rendering_val):
        fix_url(event.rendering_val['data'],
                event['request'].application_url,
                event['request'].registry.settings)


def fix_url(item, app_url, settings={}):
    if isinstance(item, list):
        [
            fix_url(i, app_url, settings)
            for i in item
            if isinstance(i, dict) or isinstance(i, list)
        ]
    elif isinstance(item, dict):
        if "format" in item and "url" in item and '?download=' in item['url']:
            path = item["url"] if item["url"].startswith('/') else '/' + '/'.join(item['url'].split('/')[5:])
            item["url"] = app_url + route_prefix(settings) + path
            return
        [
            fix_url(item[i], app_url, settings)
            for i in item
            if isinstance(item[i], dict) or isinstance(item[i], list)
        ]


def encrypt(uuid, name, key):
    iv = "{:^{}.{}}".format(name, AES.block_size, AES.block_size)
    text = "{:^{}}".format(key, AES.block_size)
    return hexlify(AES.new(uuid, AES.MODE_CBC, iv).encrypt(text))


def decrypt(uuid, name, key):
    iv = "{:^{}.{}}".format(name, AES.block_size, AES.block_size)
    try:
        text = AES.new(uuid, AES.MODE_CBC, iv).decrypt(unhexlify(key)).strip()
    except:
        text = ''
    return text
