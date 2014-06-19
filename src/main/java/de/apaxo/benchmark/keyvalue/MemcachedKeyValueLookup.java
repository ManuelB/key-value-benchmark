package de.apaxo.benchmark.keyvalue;

import java.util.Map;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

/**
 * run "sudo apt-get install memcached" before usage
 *
 */

public class MemcachedKeyValueLookup implements KeyValueLookup {

    
    static {
        String[] serverlist = { "localhost:11211" };
        SockIOPool pool = SockIOPool.getInstance();
        pool.setServers(serverlist);
        pool.initialize();
    }
    
    private MemCachedClient memCachedClient;
    
    public void init(Map<String, String> configuration) {
        memCachedClient = new MemCachedClient();
    }

    public void cleanup() {
        SockIOPool.getInstance().shutDown();
        memCachedClient = null;

    }

    public Object lookup(String key) {
        return memCachedClient.get(key);
    }

    public void remove(String key) {
        memCachedClient.delete(key);

    }

    public void insert(String key, Object o) {
        memCachedClient.set(key, o);
    }
    
    public String toString() {
        return "MemcachedKeyValueLookup";
    }

}
