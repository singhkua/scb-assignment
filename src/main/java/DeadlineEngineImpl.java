import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/*
 * The class uses ConcurrentHashMap to store the deadlines.
 *
 * The implementation sets the cancel flag for the cancelled deadlines. The poll checks this flag before firing a deadline
 * This ensures even if the poll is in progress and deadline has not been fired, if the user cancels the deadline, it wont get fired.
 *
 * Concerns -
 * 1. It does not stop the user to overload the class with deadlines and will run out of memory.
 * 2. Does not check for -ive deadlineMs, should that be allowed? Same for nowMs in Poll method
 * 3. What should happen to the raised deadlines, should they be removed?
 * 4. The size method wont be 100% accurate. --  it will be the case with any multi-threaded heavily used program.
 * 5. Exception handling by the Consumer<Long> handler is not done
 *
 * 6. You have asked program to be production ready, if you meant about the test cases, the coverage is good.
 * But the program will not be production ready without knowing the conditions of the production and handling memory effectively
 * */

public class DeadlineEngineImpl implements DeadlineEngine {
    private final AtomicLong requestIds = new AtomicLong(0);
    private final Map<Long, ScheduledDeadline> deadlineMap = new ConcurrentHashMap<>();

    @Override
    public long schedule(long deadlineMs) {
        // Get the unique requestId
        long requestId = requestIds.getAndIncrement();

        deadlineMap.put(requestId, new ScheduledDeadline(deadlineMs));

        return requestId;
    }

    @Override
    public boolean cancel(long requestId) {
        ScheduledDeadline deadline = deadlineMap
                .computeIfPresent(requestId, (aLong, scheduledDeadline) -> scheduledDeadline.cancel());
        return deadline != null;
    }

    /*
     * Removes the cancelled deadlines before firing the active ones.
     * */
    @Override
    public int poll(long nowMs, Consumer<Long> handler, int maxPoll) {
        if (null == handler) {
            throw new IllegalArgumentException("the parameter Handler to the poll method cannot be null");
        }
        if (maxPoll < 0) {
            throw new IllegalArgumentException("the maxPoll must be positive");
        }

        this.deadlineMap.values().removeIf(ScheduledDeadline::isCancelled);

        return (int) this.deadlineMap.entrySet().stream()
                .filter(es -> es.getValue().deadlineMs < nowMs) // Find expired deadlines
                .filter(es -> es.getValue().isNotRaisedAndNotCancelled()) // Make sure they are not fired already
                .limit(maxPoll) // maximum firing allowed
                .peek(es -> handleExpiredUnRaisedDeadline(es.getKey(), es.getValue(), handler))
                .count();
    }

    private void handleExpiredUnRaisedDeadline(Long requestId, ScheduledDeadline deadline, Consumer<Long> handler) {
        if (deadline.isRaisedOrCancelled()) {
            return;
        }

        // Synchronizing on the Request Key Making sure schedule gets fired only once.
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (requestId) {
            if (deadline.isNotRaisedAndNotCancelled()) {
                handler.accept(requestId);
                deadline.setRaised(); // set the flag to fired
            }
        }
    }

    @Override
    public int size() {
        return (int) deadlineMap.values().stream()
                .filter(ScheduledDeadline::isNotCancelled)
                .count();
    }

    /*
     * An internal class to store the state and timing of the Deadline
     * */
    private static final class ScheduledDeadline {
        private final long deadlineMs;
        private boolean raised;
        private boolean cancelled;

        private ScheduledDeadline(long deadlineMs) {
            this.deadlineMs = deadlineMs;
        }

        private void setRaised() {
            this.raised = true;
        }

        private boolean isNotRaisedAndNotCancelled() {
            return !raised && !cancelled;
        }

        private boolean isRaisedOrCancelled() {
            return raised || cancelled;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public boolean isNotCancelled() {
            return !cancelled;
        }

        public ScheduledDeadline cancel() {
            this.cancelled = true;
            return this;
        }
    }
}
