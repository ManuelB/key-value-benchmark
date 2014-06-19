package de.apaxo.benchmark.keyvalue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapKeyValueLookup extends HashMapKeyValueLookup {

    @Override
    public void init(Map<String, String> map) {
        setMap(new ConcurrentHashMap<String, Object>());
    }
    
    public String toString() {
        return "ConcurrentHashMapKeyValueLookup";
    }

}
