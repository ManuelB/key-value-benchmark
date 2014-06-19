package de.apaxo.benchmark.keyvalue.run;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.apaxo.benchmark.keyvalue.ConcurrentHashMapKeyValueLookup;
import de.apaxo.benchmark.keyvalue.HashMapKeyValueLookup;
import de.apaxo.benchmark.keyvalue.KeyValueLookup;
import de.apaxo.benchmark.keyvalue.MemcachedKeyValueLookup;
import de.apaxo.benchmark.keyvalue.MySQLKeyValueLookup;

public class BenchmarkRunner {

    private static enum BenchmarkAction {
        Insert, Remove, Lookup
    }
    
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
        System.out.println("Running benchmark. Press any key to continue.");
        System.in.read();

        Map<Class<?>, Map<String, String>> keyLookupClasses = new LinkedHashMap<Class<?>, Map<String, String>>();

        //Map<String, String> hashMapKeyValueConfiguration = new HashMap<String, String>();
        //keyLookupClasses.put(HashMapKeyValueLookup.class,
        //        hashMapKeyValueConfiguration);
        
        Map<String, String> concurrentHashMapKeyValueConfiguration = new HashMap<String, String>();
        keyLookupClasses.put(ConcurrentHashMapKeyValueLookup.class,
                concurrentHashMapKeyValueConfiguration);

        Map<String, String> memcachedKeyValueConfiguration = new HashMap<String, String>();
        keyLookupClasses.put(MemcachedKeyValueLookup.class,
                memcachedKeyValueConfiguration);

        Map<String, String> mySQLKeyValueLookupConfiguration = new HashMap<String, String>();
        keyLookupClasses.put(MySQLKeyValueLookup.class,
                mySQLKeyValueLookupConfiguration);

        ExecutorService executor = Executors.newFixedThreadPool(16);
        for (Entry<Class<?>, Map<String, String>> clazzEntry : keyLookupClasses
                .entrySet()) {
            final KeyValueLookup keyValueLookup = (KeyValueLookup) clazzEntry
                    .getKey().newInstance();
            keyValueLookup.init(clazzEntry.getValue());
            benchmark(keyValueLookup, executor);
            keyValueLookup.cleanup();

        }
        executor.shutdown();

    }

    private static void benchmark(KeyValueLookup keyValueLookup, ExecutorService executor) throws InterruptedException, ExecutionException {

        int numObjects = 100000;
        System.out.println("Running benchmark for '" + numObjects
                + "' objects of type 'String' with implementation '"
                + keyValueLookup + "'");

        // Init test Objects
        //
        Map<String,String> objects = new HashMap<String, String>(numObjects);
        for (int i = 0; i < numObjects; i++) {
            objects.put(UUID.randomUUID().toString(), new String("Test value " + i));
        }

        LinkedHashMap<Integer, LinkedHashMap<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>>> resultMap = new LinkedHashMap<Integer, LinkedHashMap<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>>>();

        

        // Insert
        doAction(BenchmarkAction.Insert, keyValueLookup, objects, resultMap, executor);

        // Lookup
        doAction(BenchmarkAction.Lookup, keyValueLookup, objects, resultMap, executor);

        // Remove
        doAction(BenchmarkAction.Remove, keyValueLookup, objects, resultMap, executor);

        

        System.out.println("\n\n");
        System.out.println("Results at a glance");
        System.out.println("====================");
        for (Entry<Integer, LinkedHashMap<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>>> entry : resultMap
                .entrySet()) {
            System.out.println("Run with '" + entry.getKey() + "' objects: ");
            for (Entry<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>> typeEntry : entry
                    .getValue().entrySet()) {

                System.out.println(typeEntry.getKey().getClass().getName()
                        + ":");
                System.out.println("-----------------------");
                for (Entry<BenchmarkAction, Long> timeEntry : typeEntry
                        .getValue().entrySet()) {
                    System.out.println(timeEntry.getKey().toString() + ": "
                            + timeEntry.getValue() + "ms");
                }
                System.out.println();
            }
        }
    }

    private static void doAction(
            final BenchmarkAction action,
            final KeyValueLookup keyValueLookup,
            Map<String,String> objects,
            LinkedHashMap<Integer, LinkedHashMap<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>>> resultMap,
            ExecutorService executor) throws InterruptedException, ExecutionException {

        long startTime = System.currentTimeMillis();

        List<Future<?>> waitForAll = new ArrayList<Future<?>>();
        
        
       
        for(Entry<String,String> entry : objects.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            waitForAll.add(executor.submit(new Runnable() {
                public void run() {
                    switch (action) {
                    case Insert:
                        keyValueLookup.insert(key,
                                value);
                        break;
                    case Remove:
                        keyValueLookup.remove(key);
                        break;
                    case Lookup:
                        keyValueLookup.lookup(key);
                        break;

                    default:
                        throw new UnsupportedOperationException();

                    }
                }
            }));

        }
        
        for(Future<?> taskTermination : waitForAll) {
            taskTermination.get();
        }
        
        
        long duration = (System.currentTimeMillis() - startTime);

        int numObjects = objects.entrySet().size();
        if (resultMap.get(numObjects) == null) {
            LinkedHashMap<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>> timesPerActionForImpl = new LinkedHashMap<KeyValueLookup, LinkedHashMap<BenchmarkAction, Long>>();
            LinkedHashMap<BenchmarkAction, Long> times = new LinkedHashMap<BenchmarkRunner.BenchmarkAction, Long>();
            times.put(action, duration);
            timesPerActionForImpl.put(keyValueLookup, times);
            resultMap.put(numObjects, timesPerActionForImpl);
        } else {

            if (resultMap.get(numObjects).get(keyValueLookup) == null) {
                LinkedHashMap<BenchmarkAction, Long> times = new LinkedHashMap<BenchmarkRunner.BenchmarkAction, Long>();
                times.put(action, duration);
                resultMap.get(numObjects).put(keyValueLookup, times);

            } else {
                resultMap.get(numObjects).get(keyValueLookup)
                        .put(action, duration);
            }
        }
    }

}
