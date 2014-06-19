/**
 * 
 */
package de.apaxo.benchmark.keyvalue;

import java.util.Map;

/**
 * @author hu
 *
 */
public interface KeyValueLookup {

    /**
     * Does some initialiization for
     * the key value lookup system.
     * e.g. opening database connection, creating
     * tables, init data
     * @param configuration TODO
     */
    public void init(Map<String, String> configuration);
    
    /**
     * Do some cleanup like
     * deleting data or deleting tables.
     */
    public void cleanup();
    
    /**
     * Lookup the object saved with given key
     * @param key
     * @return
     */
    public Object lookup(String key);
    
    /**
     * Remove the given object from the store.
     * @param key
     */
    public void remove(String key);
    
    /**
     * Insert the object into the key value store.
     * 
     * @param key
     * @param o
     */
    public void insert(String key, Object o);
}
