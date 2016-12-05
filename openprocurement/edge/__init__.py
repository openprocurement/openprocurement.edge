# -*- coding: utf-8 -*-
"""Main entry point
"""
if 'test' not in __import__('sys').argv[0]:
    import gevent.monkey
    gevent.monkey.patch_all()
import os
from couchdb import Server as CouchdbServer, Session
from couchdb.http import Unauthorized, extract_credentials
from logging import getLogger
from openprocurement.edge.utils import extract_tender, extract_auction, extract_contract, extract_plan, add_logging_context, set_logging_context

from openprocurement.api.design import sync_design
from openprocurement.api.utils import request_params, set_renderer, beforerender

try:
    import openprocurement.auctions.core as auctions_core
    from openprocurement.auctions.core.design import add_design as add_auction_design
except ImportError:
     auctions_core = None
try:
    import openprocurement.contracting.api as contracting
    from openprocurement.contracting.api.design import add_design as add_contract_design
except ImportError:
     contracting = None
try:
    import openprocurement.planning.api as planning
    from openprocurement.planning.api.design import add_design as add_plan_design
except ImportError:
     planning = None

from pbkdf2 import PBKDF2
from pyramid.config import Configurator
from pyramid.events import NewRequest, BeforeRender, ContextFound
from pyramid.renderers import JSON, JSONP
from pyramid.settings import asbool

LOGGER = getLogger("{}.init".format(__name__))
VALIDATE_DOC_ID = '_design/_auth'
VALIDATE_DOC_UPDATE = """function(newDoc, oldDoc, userCtx){
    if(newDoc._deleted && newDoc.tenderID) {
        throw({forbidden: 'Not authorized to delete this document'});
    }
    if(userCtx.roles.indexOf('_admin') !== -1 && newDoc._id.indexOf('_design/') === 0) {
        return;
    }
    if(userCtx.name === '%s') {
        return;
    } else {
        throw({forbidden: 'Only authorized user may edit the database'});
    }
}"""


class Server(CouchdbServer):
    _uuid = None

    @property
    def uuid(self):
        """The uuid of the server.

        :rtype: basestring
        """
        if self._uuid is None:
            _, _, data = self.resource.get_json()
            self._uuid = data['uuid']
        return self._uuid


def main(global_config, **settings):
    version = settings.get('api_version')
    route_prefix = '/api/{}'.format(version)
    config = Configurator(
        autocommit=True,
        settings=settings,
        route_prefix=route_prefix,
    )
    config.include('pyramid_exclog')
    config.include("cornice")
    config.add_request_method(request_params, 'params', reify=True)
    config.add_renderer('prettyjson', JSON(indent=4))
    config.add_renderer('jsonp', JSONP(param_name='opt_jsonp'))
    config.add_renderer('prettyjsonp', JSONP(indent=4, param_name='opt_jsonp'))
    config.add_subscriber(add_logging_context, NewRequest)
    config.add_subscriber(set_logging_context, ContextFound)
    config.add_subscriber(set_renderer, NewRequest)
    config.add_subscriber(beforerender, BeforeRender)
    config.scan("openprocurement.edge.views.spore")
    config.scan("openprocurement.edge.views.health")

    resources = settings.get('resources') and settings['resources'].split(',')

    if 'tenders' in resources:
        config.scan("openprocurement.edge.views.tenders")
        config.add_request_method(extract_tender, 'tender', reify=True)

    if 'auctions' in resources and auctions_core:
        config.add_request_method(extract_auction, 'auction', reify=True)
        config.scan("openprocurement.edge.views.auctions")
        add_auction_design()

    if 'contracts' in resources and contracting:
        config.add_request_method(extract_contract, 'contract', reify=True)
        config.scan("openprocurement.edge.views.contracts")
        add_contract_design()

    if 'plans' in resources and planning:
        config.add_request_method(extract_plan, 'plan', reify=True)
        config.scan("openprocurement.edge.views.plans")
        add_plan_design()

    # CouchDB connection
    db_name = os.environ.get('DB_NAME', settings['couchdb.db_name'])
    server = Server(settings.get('couchdb.url'), session=Session(retry_delays=range(10)))
    if 'couchdb.admin_url' not in settings and server.resource.credentials:
        try:
            server.version()
        except Unauthorized:
            server = Server(extract_credentials(settings.get('couchdb.url'))[0])
    config.registry.couchdb_server = server
    if db_name not in server:
        server.create(db_name)
    db = server[db_name]
    # sync couchdb views
    sync_design(db)
    config.registry.db = db

    config.registry.server_id = settings.get('id', '')
    config.registry.health_threshold = float(settings.get('health_threshold', 99))
    config.registry.api_version = version
    config.registry.update_after = asbool(settings.get('update_after', True))
    return config.make_wsgi_app()
