function(doc) {
    if(doc.doc_type == 'Auction') {
        emit(doc.auctionID, null);
    }
}