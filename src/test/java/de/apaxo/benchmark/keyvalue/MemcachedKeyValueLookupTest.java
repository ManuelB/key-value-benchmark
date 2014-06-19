package de.apaxo.benchmark.keyvalue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.whalin.MemCached.SockIOPool;


public class MemcachedKeyValueLookupTest {

    private static MemcachedKeyValueLookup memcachedKeyValueLookup;
   
    @BeforeClass
    public static void setUp() {
        memcachedKeyValueLookup = new MemcachedKeyValueLookup();
        memcachedKeyValueLookup.init(null);
    }
    
    @Test
    public void test() {
        memcachedKeyValueLookup.insert("test", "test2");
        assertEquals("test2", memcachedKeyValueLookup.lookup("test"));
        
        memcachedKeyValueLookup.remove("test");
        assertNull(memcachedKeyValueLookup.lookup("test"));
        
        
    }
    
    @AfterClass
    public static void tearDown() {
        memcachedKeyValueLookup.cleanup();
    }

}
