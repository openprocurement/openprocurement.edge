# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging
import logging.config
from couchdb import Server, Session
from socket import error
from urlparse import urlparse
import gevent.pool
from gevent import spawn, sleep, idle

logger = logging.getLogger(__name__)


class LogsCollectorConfigError(Exception):
    pass


class LogsCollector(object):
    """Logs Collector"""

    def __init__(self, config):
        super(LogsCollector, self).__init__()
        self.config = config
        self.storage = self.config_get('storage')
        if not self.storage:
            raise LogsCollectorConfigError('Configuration Error: Missing logs'
                                           ' storage.')
        if self.storage == 'couchdb':
            self.couch_url = self.config_get('couch_url')
            self.db_name = self.config_get('log_db')
            if not self.couch_url:
                raise LogsCollectorConfigError('Configuration Error: Missing '
                                               'couch_url')
            else:
                couch_url = urlparse(self.couch_url)
                if couch_url.scheme == '' or couch_url.netloc == '':
                    raise LogsCollectorConfigError('Configuration Error:'
                                                   'Invalid couch_url')
            if not self.db_name:
                raise LogsCollectorConfigError('ConnectionError: Missing '
                                               'couchdb name')
            server = Server(self.couch_url,
                            session=Session(retry_delays=range(10)))
            try:
                if self.db_name not in server:
                    self.db = server.create(self.db_name)
                else:
                    self.db = server[self.db_name]
            except error as e:
                logger.error('Database error: {}'.format(e.message))
                raise LogsCollectorConfigError(e.message)

    def save(self, log_document):
        self.db.save(log_document)
        logger.info(self.dict_to_str_vpl(log_document))

    def dict_to_str_vpl(self, d):
        return_str = ''
        for k, v in d.items():
            return_str += str(k) + ' ' + str(v) + '\n'
        return return_str

    def config_get(self, name):
        try:
            return self.config.get('main').get(name)
        except AttributeError as e:
            raise LogsCollectorConfigError('In config dictionary missed'
                                           ' section \'main\'')

    def run(self):
        pass


def main():
    parser = argparse.ArgumentParser(description='---- Logs Collector----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        LogsCollector(config).run()


##############################################################

if __name__ == "__main__":
    main()
