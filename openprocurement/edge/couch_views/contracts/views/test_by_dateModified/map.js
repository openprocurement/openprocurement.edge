function(doc) {
    if(doc.doc_type == 'Contract' && doc.mode == 'test') {
        var fields=['contractID'], data={};
        for (var i in fields) {
            if (doc[fields[i]]) {
                data[fields[i]] = doc[fields[i]]
            }
        }
        emit(doc.dateModified, data);
    }
}