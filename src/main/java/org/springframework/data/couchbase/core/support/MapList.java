package org.springframework.data.couchbase.core.support;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MapList<K,V> extends HashMap<K, List<V>> {
  public V getOne(K key){
    List<V> list=this.get(key);
    if(list == null){
      return null;
    }
    if(list.size() > 1){
      throw new IllegalArgumentException("there is more than one for key"+key);
    }
    return list.get(0);
  }

  public void putOne(K key, V value){
    List<V> list = this.get(key);
    if ( list == null){
      this.put(key, list = new LinkedList<V>());
    }
    list.add(value);
  }
}
