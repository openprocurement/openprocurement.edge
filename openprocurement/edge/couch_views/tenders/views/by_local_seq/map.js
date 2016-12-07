function(doc) {
    if(doc.doc_type == 'Tender' && doc.status != 'draft') {
        var fields=['auctionPeriod', 'status', 'tenderID', 'lots', 'procurementMethodType', 'next_check', 'dateModified'], data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc._local_seq, data);
    }
}