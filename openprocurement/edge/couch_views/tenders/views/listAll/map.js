function(doc) {
    if(doc.doc_type == 'Tender') {
        emit(doc.dateModified, null);
    }
}