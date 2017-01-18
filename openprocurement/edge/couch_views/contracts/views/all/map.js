function(doc) {
    if(doc.doc_type == 'Contract') {
        emit(doc.contractID, null);
    }
}