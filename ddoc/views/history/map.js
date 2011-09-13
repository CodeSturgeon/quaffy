function(doc){
  if (doc.downloads){
    var folders = {};
    doc.downloads.map(function(dl){
        var folder = dl.path.split('/').slice(2,3)[0];
        if (!folders.hasOwnProperty(folder)) { folders[folder]=0; }
        folders[folder] += 1;
    });
    for (var folder in folders){
      emit([doc.profile, doc.timestamp], folder);
    }
  }
}
