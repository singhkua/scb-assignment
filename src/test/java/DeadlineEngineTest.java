import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class DeadlineEngineTest {

    @Test
    void shouldAssignUniqueRequestId_whenDeadlineIsScheduled() throws InterruptedException {
        DeadlineEngine engine = new DeadlineEngineImpl();
        HashSet<Long> requestIdsSet = new HashSet<>();
        LocalDateTime now = LocalDateTime.now();

        // schedule 10k deadlines in parallel
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Callable<Long>> callableList = IntStream.range(1, 10_000)
                .mapToObj(i -> (Callable<Long>) () -> engine.schedule(now.plusSeconds(i).toEpochSecond(ZoneOffset.UTC)))
                .collect(Collectors.toList());
        List<Future<Long>> futures = executor.invokeAll(callableList);
        futures.forEach(f -> {
            try {
                requestIdsSet.add(f.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
        executor.shutdown();

        int size = engine.size();
        assertThat(size, equalTo(9999));
        assertThat(requestIdsSet.size(), equalTo(9999));
    }

    @Test
    void shouldRaiseMaximumDeadlinesEqualToMaxPoll_whenMaxPollParameterIsSet() {
        DeadlineEngine engine = new DeadlineEngineImpl();

        // initialize with 10k schedules
        IntStream.range(1, 10_001)
                .forEach(i -> engine.schedule(LocalDateTime.now().plusSeconds(i).toEpochSecond(ZoneOffset.UTC)));

        // Fire only 4k with consumerA
        Consumer<Long> consumerA = mockConsumer();
        int fired = engine.poll(LocalDateTime.now().plusSeconds(10_000).toEpochSecond(ZoneOffset.UTC),
                consumerA, 4_000);

        assertThat(fired, equalTo(4000));
        verify(consumerA, times(4000)).accept(anyLong());

        // Fire the rest 6k with consumerB
        Consumer<Long> consumerB = mockConsumer();
        int firedAgain = engine.poll(LocalDateTime.now().plusSeconds(10_001).toEpochSecond(ZoneOffset.UTC), consumerB, 10_000);

        assertThat(firedAgain, equalTo(6000));
        verify(consumerB, times(6000)).accept(anyLong());
    }

    @Test
    void shouldOnlyRaiseTheDeadline_whenTheDeadlineHasExpired() {
        DeadlineEngine engine = new DeadlineEngineImpl();
        LocalDateTime now = LocalDateTime.now();

        // Register 10K deadlines, each expiring every 1 additional second from now
        IntStream.range(1, 10_001)
                .forEach(i -> engine.schedule(now.plusSeconds(i).toEpochSecond(ZoneOffset.UTC)));

        // Expire 4K deadlines, by passing expiring time to now + 4k
        Consumer<Long> consumerA = mockConsumer();
        int fired = engine.poll(now.plusSeconds(4000).toEpochSecond(ZoneOffset.UTC), consumerA, 6_000);

        // Expecting all deadlines < 4k to be fired
        assertThat(fired, equalTo(3999));
        verify(consumerA, times(3999)).accept(anyLong());
    }

    @Test
    void shouldRaiseDeadlineOnlyOnce_whenPolledInParallel() throws InterruptedException {
        DeadlineEngine engine = new DeadlineEngineImpl();
        LocalDateTime now = LocalDateTime.now();

        // Create 100K Schedules
        IntStream.range(1, 100_001)
                .forEach(i -> engine.schedule(now.plusSeconds(i).toEpochSecond(ZoneOffset.UTC)));

        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Fire lots (500) of parallel polls, allowing 1000 deadlines in each poll. Total of 500K deadlines can be fired
        Consumer<Long> consumerA = mockConsumer();
        List<Callable<Integer>> callableList = IntStream.range(1, 500)
                .mapToObj(i -> (Callable<Integer>) () -> engine.poll(now.plusSeconds(1000_001).toEpochSecond(ZoneOffset.UTC), consumerA, 1_000))
                .collect(Collectors.toList());
        executor.invokeAll(callableList);
        executor.shutdown();

        // Expect exact 100 deadlines to be fired. Assuring each deadline only fired once
        verify(consumerA, times(100_000)).accept(anyLong());
    }

    @Test
    void shouldNotRaiseTheDeadline_whenTheDeadlineIsCancelled() throws InterruptedException {
        DeadlineEngine engine = new DeadlineEngineImpl();

        LocalDateTime now = LocalDateTime.now();
        IntStream.range(1, 100_001)
                .forEach(i -> engine.schedule(now.plusSeconds(i).toEpochSecond(ZoneOffset.UTC)));

        ExecutorService executor = Executors.newFixedThreadPool(4);

        Consumer<Long> consumerA = mockConsumer();
        List<Callable<Integer>> callableList = IntStream.range(1, 500)
                .mapToObj(i -> (Callable<Integer>) () -> engine.poll(now.plusSeconds(1000_001).toEpochSecond(ZoneOffset.UTC), consumerA, 1_000))
                .collect(Collectors.toList());

        executor.execute(() -> IntStream.range(25000, 50000).forEach(engine::cancel));
        executor.execute(() -> IntStream.range(50000, 75000).forEach(engine::cancel));
        executor.execute(() -> IntStream.range(75000, 100000).forEach(engine::cancel));
        executor.invokeAll(callableList);
        executor.shutdown();

        verify(consumerA, atMost(49_000)).accept(anyLong());
    }

    @Test
    void shouldCancelTheDeadline_whenCancelled() {
        DeadlineEngine engine = new DeadlineEngineImpl();
        long requestId = engine.schedule(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        assertThat(engine.size(), equalTo(1));

        engine.cancel(requestId);
        assertThat(engine.size(), equalTo(0));
    }

    @Test
    void shouldDoNothing_whenCancelledWithInvalidRequestId() {
        DeadlineEngine engine = new DeadlineEngineImpl();
        long requestId = engine.schedule(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        assertThat(engine.size(), equalTo(1));

        engine.cancel(requestId + 1);
        assertThat(engine.size(), equalTo(1));
    }


    @Test
    void shouldThrowIllegalArgument_whenConsumerFunctionIsNull() {
        DeadlineEngine engine = new DeadlineEngineImpl();
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> engine.poll(0, null, 0));

        assertThat(exception.getMessage(), equalTo("the parameter Handler to the poll method cannot be null"));
    }

    @Test
    void shouldThrowIllegalArgument_whenMaxPollIsNegative() {
        DeadlineEngine engine = new DeadlineEngineImpl();
        Consumer<Long> consumer = mockConsumer();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            engine.poll(0, consumer, -1);
        });

        assertThat(exception.getMessage(), equalTo("the maxPoll must be positive"));
    }

    @Test
    @Disabled
    void shouldThrowException_whenConsumerFunctionThrowsException() {
    }

    @SuppressWarnings("unchecked")
    private Consumer<Long> mockConsumer() {
        return mock(Consumer.class);
    }
}
