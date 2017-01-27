# -*- coding: utf-8 -*-
if 'test' not in __import__('sys').argv[0]:
    import gevent.monkey
    gevent.monkey.patch_all()
import os
from couchdb import Server as CouchdbServer, Session
from couchdb.http import Unauthorized, extract_credentials
from logging import getLogger
from openprocurement.edge.utils import add_logging_context, set_logging_context
from openprocurement.edge.utils import push_views, beforerender
from openprocurement.edge.utils import request_params, set_renderer

LOGGER = getLogger("{}.init".format(__name__))

from pbkdf2 import PBKDF2
from pyramid.config import Configurator
from pyramid.events import NewRequest, BeforeRender, ContextFound
from pyramid.renderers import JSON, JSONP
from pyramid.settings import asbool

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


VALIDATE_BULK_DOCS_ID = '_design/validate_date_modified'
VALIDATE_BULK_DOCS_UPDATE = """function(newDoc, oldDoc, userCtx) {
    if (oldDoc && (newDoc.dateModified <= oldDoc.dateModified)) {
        throw({forbidden: 'New doc with oldest dateModified.' });
    };
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
    couchapp_path = os.path.dirname(os.path.abspath(__file__)) + '/couch_views'
    couch_url = settings.get('couchdb.url') + settings.get('couchdb.db_name')
    if 'tenders' in resources:
        config.scan("openprocurement.edge.views.tenders")
        push_views(couchapp_path=couchapp_path+'/tenders', couch_url=couch_url)
        LOGGER.info('Push couch tenders views successful.')
        LOGGER.info('Tender resource initialized successful.')

    if 'auctions' in resources:
        config.scan("openprocurement.edge.views.auctions")
        push_views(couchapp_path=couchapp_path+'/auctions', couch_url=couch_url)
        LOGGER.info('Push couch auctions views successful.')
        LOGGER.info('Auction resource initialized successful.')

    if 'contracts' in resources:
        config.scan("openprocurement.edge.views.contracts")
        push_views(couchapp_path=couchapp_path+'/contracts',
                   couch_url=couch_url)
        LOGGER.info('Push couch contracts views successful.')
        LOGGER.info('Contract resource initialized successful.')

    if 'plans' in resources:
        config.scan("openprocurement.edge.views.plans")
        push_views(couchapp_path=couchapp_path+'/plans', couch_url=couch_url)
        LOGGER.info('Push couch plans views successful.')
        LOGGER.info('Plan resource initialized successful.')
    # CouchDB connection
    db_name = os.environ.get('DB_NAME', settings['couchdb.db_name'])
    server = Server(settings.get('couchdb.url'),
                    session=Session(retry_delays=range(10)))
    config.registry.couchdb_server = server
    if db_name not in server:
        server.create(db_name)
    db = server[db_name]
    validate_doc = db.get(VALIDATE_BULK_DOCS_ID, {'_id': VALIDATE_BULK_DOCS_ID})
    if validate_doc.get('validate_doc_update') != VALIDATE_BULK_DOCS_UPDATE:
        validate_doc['validate_doc_update'] = VALIDATE_BULK_DOCS_UPDATE
        db.save(validate_doc)
    # sync couchdb views
    # sync_design(db)
    config.registry.db = db
    config.registry.server_id = settings.get('id', '')
    config.registry.health_threshold = float(settings.get('health_threshold', 99))
    config.registry.api_version = version
    config.registry.update_after = asbool(settings.get('update_after', True))
    return config.make_wsgi_app()
