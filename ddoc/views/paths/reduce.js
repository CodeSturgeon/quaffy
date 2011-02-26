function(keys, values, rereduce){
  var best;
  var ts = '';
  for (var vi in values) {
    var value = values[vi];
    if (value[2] > ts) {
      ts = value[2];
      best = value;
    }
  }
  return best;
}
