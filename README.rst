.. image:: https://travis-ci.org/openprocurement/openprocurement.edge.svg?branch=master
    :target: https://travis-ci.org/openprocurement/openprocurement.edge

.. image:: https://coveralls.io/repos/github/openprocurement/openprocurement.edge/badge.svg?branch=master
    :target: https://coveralls.io/github/openprocurement/openprocurement.edge?branch=master

.. image:: https://img.shields.io/hexpm/l/plug.svg
    :target: https://github.com/openprocurement/openprocurement.edge/blob/master/LICENSE.txt


openprocurement.edge
====================

**openprocurement.edge** enables synchronization with the Central database that is one of the main components of *OpenProcurement*, an open source software toolkit, implemented in *ProZorro, ProZorro.sale, Rialto, MTender* e-procurement systems.

*OpenProcurement* contains a Central database (CDB) and an API (REST-ful interface based on the JSON notation) via which specialized commercial web platforms can interact with CDB. The openprocurement.edge package enables storage of a personal up-to-date copy of public data from the CDB, being an analogue of public access point (with the same GET request methods available as for a public point), and provides data about any lag compared to the CDB.
