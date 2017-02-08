# -*- coding: utf-8 -*-
if 'test' not in __import__('sys').argv[0]:
    import gevent.monkey
    gevent.monkey.patch_all()
from couchdb import Server as CouchdbServer, Session
from logging import getLogger
from openprocurement.edge.utils import (
    add_logging_context,
    set_logging_context,
    prepare_couchdb,
    prepare_couchdb_views,
    beforerender,
    request_params,
    set_renderer
)

LOGGER = getLogger("{}.init".format(__name__))

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
    couch_url = settings.get('couchdb.url') + settings.get('couchdb.db_name')
    for resource in resources:
        config.scan("openprocurement.edge.views." + resource)
        prepare_couchdb_views(couch_url, resource, LOGGER)
        LOGGER.info('Push couch {} views successful.'.format(resource))
        LOGGER.info('{} resource initialized successful.'.format(resource.title()))

    # CouchDB connection
    server = Server(settings.get('couchdb.url'),
                    session=Session(retry_delays=range(10)))
    config.registry.couchdb_server = server
    config.registry.db = prepare_couchdb(settings.get('couchdb.url'),
                                         settings.get('couchdb.db_name'),
                                         LOGGER)
    config.registry.server_id = settings.get('id', '')
    config.registry.health_threshold = float(settings.get('health_threshold', 99))
    config.registry.api_version = version
    config.registry.update_after = asbool(settings.get('update_after', True))
    return config.make_wsgi_app()
