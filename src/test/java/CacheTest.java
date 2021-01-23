import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class CacheTest {

    @Test
    void shouldInvokeFunctionEachTime_whenANewUniqueKeyIsLookedUpAndCacheMiss() throws InterruptedException {
        Function<String, String> function = mockFunction();
        when(function.apply(anyString())).thenReturn("Value");

        Cache<String, String> cache = new CacheImpl<>(function);

        // Creating 10K unique keys to be invoked in parallel
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Callable<String>> callableList = IntStream.range(1, 10000)
                .mapToObj(i -> (Callable<String>) () -> cache.get(String.format("K%s", i)))
                .collect(Collectors.toList());
        executor.invokeAll(callableList);
        executor.shutdown();

        // Expecting method to be raised for each unique key
        verify(function, times(9999)).apply(anyString());
        // Expecting method to be raised once key K1
        verify(function, times(1)).apply("K1");
        // Expecting method to be raised once key K9999
        verify(function, times(1)).apply("K9999");
    }

    @Test
    void shouldInvokeFunctionOnlyOnce_whenSameKeyIsLookedUpAndCacheNotMiss() throws InterruptedException {
        Function<String, String> function = mockFunction();
        when(function.apply(anyString())).thenReturn("Value");

        Cache<String, String> cache = new CacheImpl<>(function);

        // Invoking the method 100k times for the same key
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Callable<String>> callableList = createCallableList("K", 100_000, cache);
        executor.invokeAll(callableList);
        executor.shutdown();

        // Expecting method to be raised once key K1
        verify(function, times(1)).apply("K");
    }

    @Test
    void shouldInvokeFunctionOnlyOncePerUniqueKey() throws InterruptedException {
        Function<String, String> function = mockFunction();
        when(function.apply(anyString())).thenReturn("Value");

        Cache<String, String> cache = new CacheImpl<>(function);

        // Creating 10K requests for 5 unique keys to be invoked in parallel
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Callable<String>> callableList = new ArrayList<>();
        callableList.addAll(createCallableList("K1", 10_000, cache));
        callableList.addAll(createCallableList("K2", 10_000, cache));
        callableList.addAll(createCallableList("K3", 10_000, cache));
        callableList.addAll(createCallableList("K4", 10_000, cache));
        callableList.addAll(createCallableList("K5", 10_000, cache));

        executor.invokeAll(callableList);
        executor.shutdown();

        // Expecting method to be raised once for each key

        verify(function, times(1)).apply("K1");
        verify(function, times(1)).apply("K2");
        verify(function, times(1)).apply("K3");
        verify(function, times(1)).apply("K4");
        verify(function, times(1)).apply("K5");
    }

    @Test
    void shouldThrowIllegalArgument_whenKeyIsNull() {
        Cache<String, String> cache = new CacheImpl<>(s -> s);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> cache.get(null));

        assertThat(exception.getMessage(), equalTo("the parameter Key to the get method cannot be null"));
    }

    @Test
    void shouldThrowIllegalArgument_whenFunctionIsNull() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> new CacheImpl<>(null));

        assertThat(exception.getMessage(), equalTo("the parameter function in the constructor cannot be null"));
    }

    @Test
    void shouldThrowException_whenFunctionThrowsException() {
        Function<String, String> function = mockFunction();
        when(function.apply(anyString())).thenThrow(new NullPointerException());

        Cache<String, String> cache = new CacheImpl<>(function);
        Exception exception = assertThrows(FunctionExecutionException.class, () -> cache.get("K"));

        assertThat(exception.getMessage(), equalTo("Function evaluation resulted in an exception"));
    }

    private List<Callable<String>> createCallableList(String key, int size, Cache<String, String> cache) {
        return IntStream.range(1, size)
                .mapToObj(i -> (Callable<String>) () -> cache.get(key))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private Function<String, String> mockFunction() {
        return mock(Function.class);
    }
}
