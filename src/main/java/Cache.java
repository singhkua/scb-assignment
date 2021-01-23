public interface Cache<K, V> {
    V get(K key) throws FunctionExecutionException;
}
