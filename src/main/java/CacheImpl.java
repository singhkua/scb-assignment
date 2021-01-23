import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The CacheImpl synchronize on the instance of cache key to achieve concurrency
 * <p>
 * It does not address the out of memory test case as there is no eviction policy has been implemented
 * nor keys and values wrapped in WeakReferences
 * <p>
 * The class allows the user of class to provide only 1 function per instance
 * <p>
 * Since the user has been allowed to pass a function which can throw an exception while execution,
 * the method throws an FunctionExecutionException to allow the user to handle the scenario when an exception is thrown
 *
 *  You have asked program to be production ready, if you meant about the test cases, the coverage is good.
 *  But the program will not be production ready without knowing the conditions of the production and handling memory effectively
 */

public class CacheImpl<K, V> implements Cache<K, V> {
    private final Function<K, V> function;

    // creating a map with optional value. This will handle null use case.
    private final Map<K, Optional<V>> cacheMap = new ConcurrentHashMap<>();

    CacheImpl(Function<K, V> function) {
        if (null == function) {
            throw new IllegalArgumentException("the parameter function in the constructor cannot be null");
        }
        this.function = function;
    }

    public V get(K key) throws FunctionExecutionException {
        if (null == key) {
            throw new IllegalArgumentException("the parameter Key to the get method cannot be null");
        }

        if (cacheMap.containsKey(key)) {
            return cacheMap.get(key).orElseThrow();
        }

        K lockKey = findKeyObjectFromMap(key);

        // Synchronizing on Key Object stored in Map
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (lockKey) {
            if (cacheMap.containsKey(key)) {
                return cacheMap.get(key).orElseThrow();
            }
            try {
                V value = function.apply(key);
                cacheMap.put(key, Optional.ofNullable(value));
                return value;
            } catch (Exception e) {
                throw new FunctionExecutionException("Function evaluation resulted in an exception", e);
            }
        }
    }

    /*
     * Find the key object present in the map that matches with key passed in the argument
     *
     * Return the key itself if it cannot be found.
     * */
    private K findKeyObjectFromMap(K key) {
        return cacheMap.keySet().stream()
                .filter(k -> k.hashCode() == key.hashCode()) // For keys, hashcode should match also
                .filter(k -> k.equals(key))
                .findAny()
                .orElse(key);
    }
}
