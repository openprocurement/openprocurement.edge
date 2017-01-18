function(doc) {
    if(doc.doc_type == 'Plan') {
        emit(doc.planID, null);
    }
}