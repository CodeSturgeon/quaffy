var couchapp = require('couchapp')

var ddoc = {"_id":"_design/qafd", "views":{"paths":{}}}

ddoc.views.paths.map = function(doc){
  if (doc.path) emit(doc.path, doc);
}

exports.app = ddoc;
