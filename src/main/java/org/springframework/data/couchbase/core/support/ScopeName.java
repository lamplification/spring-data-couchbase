package org.springframework.data.couchbase.core.support;

public class ScopeName {
  String name;
  public ScopeName(String name){
    this.name = name;
  }
  public String name(){
    return this.name;
  }
  public String toString(){
    return this.name();
  }
}
