function(doc) {
    if(doc.doc_type == 'Tender' && doc.status != 'draft' && !doc.mode) {
        var fields=['auctionPeriod', 'status', 'tenderID', 'lots', 'procurementMethodType', 'next_check'], data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}