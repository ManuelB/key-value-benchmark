package de.apaxo.benchmark.keyvalue;

import java.util.HashMap;
import java.util.Map;

public class HashMapKeyValueLookup implements KeyValueLookup {

    private Map<String, Object> map;
    
    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    public void init(Map<String, String> configuration) {
        map = new HashMap<String, Object>();
    }

    public void cleanup() {
        map = null;        
    }

    public Object lookup(String key) {
        return map.get(key);
    }

    public void remove(String key) {
        map.remove(key);
        
    }

    public void insert(String key, Object o) {
        map.put(key, o);
    }
    
    public String toString() {
        return "HashMapKeyValueLookup";
    }

}
