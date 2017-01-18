/**
 * Show function - use multiple `provides()` for media type-based content
 * negotiation.
 * @link http://docs.couchdb.org/en/latest/couchapp/ddocs.html#showfun
 *
 * @param {object} doc - Processed document, may be omitted.
 * @param {object} req - Request Object. http://docs.couchdb.org/en/latest/json-structure.html#request-object
 *
 * @returns {object} Response Object. http://docs.couchdb.org/en/latest/json-structure.html#response-object
 **/

function(doc, req) {
  var key;
  var ALL = "*";

  var FIELDS_TO_CLEAR = ['_id','_rev', '_revisions', 'doc_type'];


  function getField(obj) {
    if (req.query.document_id) {
      if (ALL == req.query.document_id)
        return groupDocuments(obj.documents);
      return getLastDoc(obj.documents, req.query.document_id);
    }
    return obj;
  }

  function formatResponse(data) {
    if (!data){

      if (req.query.document_id) {
        if(req.query.document_id == '*') {
          return {
            body: JSON.stringify({data:[]}),
            headers: {"Content-Type": "text/plain; charset=utf-8"}
          };
        }
         else var name = 'document_id';
     }
      else
        var name = 'contract_id';
      return {
        code: 404,
        json: {
            "status": "error",
            "errors":[{
                "location": "url",
                "name": name,
                "description": "Not found"
            }]
        }
      };
    }

    clearFields(data);

    return {
      body: JSON.stringify({data:data}),
      headers: {"Content-Type": "text/plain; charset=utf-8"}
    };
  }

  function groupDocuments(docs){
    var result = [];
    var unic = {};
    var key, i;
    if (docs){
        docs.forEach(function(item, i) {
          if (!unic[item.id] ||
               Date.parse(docs[unic[item.id]].dateModified) <
               Date.parse(item.dateModified)
             )
             unic[item.id] = i;
        });
        for (key in unic)
          result.push(docs[unic[key]]);
    }
    return result;
  }

  function getLastDoc(docs, id) {
    if (docs){
      var allDocs = [],
          length = docs.length;
      var result;

      for(;length--;)
        if (docs[length].id === id) allDocs.push(docs[length]);

      result = allDocs.length ? allDocs[0] : null;
      if (allDocs.length > 1)
        result.previousVersions = allDocs.slice(1);
    }
    return result;

  }

  function clearFields(data) {
    var key;
    for (key in FIELDS_TO_CLEAR)
      data[FIELDS_TO_CLEAR[key]] && delete data[FIELDS_TO_CLEAR[key]];
  }

  return formatResponse( getField(doc));
}
