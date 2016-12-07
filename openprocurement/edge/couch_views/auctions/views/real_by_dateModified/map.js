function(doc) {
    if(doc.doc_type == 'Auction' && doc.status != 'draft' && !doc.mode) {
        var fields=['auctionPeriod', 'status', 'auctionID', 'lots', 'procurementMethodType', 'next_check'], data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}