# -*- coding: utf-8 -*-
from couchdb.design import ViewDefinition


TENDER_FIELDS = [
    'auctionPeriod',
    'status',
    'tenderID',
    'lots',
    'procurementMethodType',
    'next_check',
]
PLAN_FIELDS = [
    'planID',
]
CONTRACT_FIELDS = [
    'contractID',
]
AUCTION_FIELDS = [
    'auctionPeriod',
    'status',
    'auctionID',
    'lots',
    'procurementMethodType',
    'next_check',
]
CHANGES_FIELDS = [
    'dateModified',
]


def add_index_options(doc):
    doc['options'] = {'local_seq': True}


def sync_design(db):
    views = [j for i, j in globals().items() if "_view" in i]
    ViewDefinition.sync_many(db, views, callback=add_index_options)


def _get_fields(resource):
    fields = None
    if resource == 'tenders':
        fields = TENDER_FIELDS
    elif resource == 'contracts':
        fields = CONTRACT_FIELDS
    elif resource == 'auctions':
        fields = AUCTION_FIELDS
    elif resource == 'plans':
        fields = PLAN_FIELDS
    return fields


def _get_changes_fields(resource):
    fields = None
    if resource == 'tenders':
        fields = TENDER_FIELDS + CHANGES_FIELDS
    elif resource == 'contracts':
        fields = CONTRACT_FIELDS + CHANGES_FIELDS
    elif resource == 'auctions':
        fields = AUCTION_FIELDS + CHANGES_FIELDS
    elif resource == 'plans':
        fields = PLAN_FIELDS + CHANGES_FIELDS
    return fields


def all_view_ViewDefinition(resource):
    return ViewDefinition(resource, 'all', '''function(doc) {
    if(doc.doc_type == '%(resource)s') {
        emit(doc.%(doc_id)sID, null);
    }
}''' % dict(resource=resource[:-1].title(), doc_id=resource[:-1]))


def by_dateModified_view_ViewDefinition(resource):
    fields = _get_fields(resource)
    return ViewDefinition(resource, 'by_dateModified', '''function(doc) {
    if(doc.doc_type == '%(resource)s' && doc.status != 'draft') {
        var fields=%(fields)s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}''' % dict(resource=resource[:-1].title(), fields=repr(fields)))


def real_by_dateModified_view_ViewDefinition(resource):
    fields = repr(_get_fields(resource))
    view_dict = {
        'fields': fields,
        'resource': resource[:-1].title()
    }
    func_source = """function(doc) {
    if(doc.doc_type == '%(resource)s' && doc.status != 'draft' && !doc.mode) {
        var fields=%(fields)s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}""" % view_dict

    return ViewDefinition(resource, 'real_by_dateModified', func_source)


def test_by_dateModified_view_ViewDefinition(resource='tenders'):
    fields = _get_fields(resource)
    return ViewDefinition(resource, 'test_by_dateModified', '''function(doc) {
    if(doc.doc_type == '%(resource)s' && doc.status != 'draft' && doc.mode == 'test') {
        var fields=%(fields)s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}''' % dict(resource=resource[:-1].title(), fields=repr(fields)))


def by_local_seq_view_ViewDefinition(resource):
    changes_fields = _get_changes_fields(resource)
    return ViewDefinition(resource, 'by_local_seq', '''function(doc) {
    if(doc.doc_type == '%(resource)s' && doc.status != 'draft') {
        var fields=%(fields)s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc._local_seq, data);
    }
}''' % dict(resource=resource[:-1].title(), fields=repr(changes_fields)))


def real_by_local_seq_view_ViewDefinition(resource):
    changes_fields = _get_changes_fields(resource)
    return ViewDefinition(resource, 'real_by_local_seq', '''function(doc) {
    if(doc.doc_type == '%(resource)s' && doc.status != 'draft' && !doc.mode) {
        var fields=%(fields)s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc._local_seq, data);
    }
}''' % dict(resource=resource[:-1].title(), fields=repr(changes_fields)))


def test_by_local_seq_view_ViewDefinition(resource='tenders'):
    changes_fields = _get_changes_fields(resource)
    return ViewDefinition(resource, 'test_by_local_seq', '''function(doc) {
    if(doc.doc_type == '%(resource)s' && doc.status != 'draft' && doc.mode == 'test') {
        var fields=%(fields)s, data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc._local_seq, data);
    }
}''' % dict(resource=resource[:-1].title(), fields=repr(changes_fields)))


conflicts_view = ViewDefinition('conflicts', 'all', '''function(doc) {
    if (doc._conflicts) {
        emit(doc._rev, [doc._rev].concat(doc._conflicts));
    }
}''')
