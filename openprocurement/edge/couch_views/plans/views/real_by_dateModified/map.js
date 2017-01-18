function(doc) {
    if(doc.doc_type == 'Plan' && !doc.mode) {
        var fields=['planID'], data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}