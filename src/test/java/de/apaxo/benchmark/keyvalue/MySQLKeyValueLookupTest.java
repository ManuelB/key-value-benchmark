package de.apaxo.benchmark.keyvalue;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MySQLKeyValueLookupTest {

    private static MySQLKeyValueLookup mysqlKeyValueLookup;
    
    @BeforeClass
    public static void setUp() {
        mysqlKeyValueLookup = new MySQLKeyValueLookup();
        mysqlKeyValueLookup.init(null);
    }
    
    @Test
    public void test() {
        
        mysqlKeyValueLookup.insert("Test", "test2");
        assertEquals("test2", mysqlKeyValueLookup.lookup("Test"));
    
        mysqlKeyValueLookup.remove("Test");
        assertNull(mysqlKeyValueLookup.lookup("Test"));
        
        
    }
 
    @AfterClass
    public static void tearDown() {
        mysqlKeyValueLookup.cleanup();
    }


}
