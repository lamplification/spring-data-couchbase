package org.springframework.data.couchbase.core.support;

public class CollectionName {
  String name;
  public CollectionName(String name){
    this.name = name;
  }
  public String name(){
    return this.name;
  }
  public String toString(){
    return this.name();
  }
}

