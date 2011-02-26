function(doc){
  if (doc.downloads) {
    for (var di in doc.downloads) {
      var dl = doc.downloads[di];
      emit([doc.profile, dl.path], [dl.mtime, dl.size, doc.timestamp]);
    }
  }
}
