import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DeadlineEngineImpl implements DeadlineEngine {
    private final TreeMap<Long, List<Long>> deadlines = new TreeMap<Long, List<Long>>((a, b) -> a.intValue() - b.intValue());
    private final ArrayList<Callback> callbackQueue = new ArrayList<>();

    private final AtomicLong deadlineCounter = new AtomicLong();

    private class Callback {
        private Consumer<Long> fn;
        private Long id;

        Callback(Consumer<Long> fn, Long id) {
            this.fn = fn;
            this.id = id;
        }

        public Consumer<Long> getFn() {
            return fn;
        }

        public Long getId() {
            return id;
        }
    }

    @Override
    public long schedule(long deadlineMs) {
        long id = deadlineCounter.incrementAndGet();
        synchronized (deadlines) {
            List<Long> r = deadlines.putIfAbsent(deadlineMs, new ArrayList<>(Arrays.asList(id)));

            if (r != null){
                deadlines.get(deadlineMs).add(id);
            }
        }
        return id;
    }

    @Override
    public boolean cancel(long requestId) {
        synchronized (deadlines) {
            // scan the deadlines
            for (Map.Entry<Long, List<Long>> entry : deadlines.entrySet()) {
                Long k = entry.getKey();
                List<Long> v = entry.getValue();
                if (v.contains(requestId)) {
                    if (v.size() == 1) {
                        deadlines.remove(k);
                        return true;
                    }
                    if (v.size() > 1) {
                        v.remove(requestId);
                        return true;
                    }
                }
            }
            return false;
        }
    }

    @Override
    public int poll(long nowMs, Consumer<Long> handler, int maxPoll) {
        NavigableMap<Long, List<Long>> m = deadlines.headMap(nowMs, true);
        synchronized (callbackQueue) {
            m.forEach((k, v) -> {
                for (Long id: v){
                    callbackQueue.add(new Callback(handler, id));
                }
            });

            // pop the first callback and exec it
            int i = 0;
            for (; i < maxPoll; i++) {
                if (callbackQueue.size() == 0) break;

                Callback cb = callbackQueue.remove(0);
                if (cb == null) return i;
                cb.getFn().accept(cb.getId());
            }

            return i;
        }
    }

    @Override
    public int size() {
        int count = 0;

        for (Map.Entry<Long, List<Long>> entry : deadlines.entrySet()) {
            count += entry.getValue().size();
        }

        return count;
    }

}
