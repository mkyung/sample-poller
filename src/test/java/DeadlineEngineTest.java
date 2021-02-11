import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.AnyOf.anyOf;

public class DeadlineEngineTest {
    @Test
    public void testAddDeadline(){
        DeadlineEngine engine = new DeadlineEngineImpl();

        long deadlineId = engine.schedule(System.currentTimeMillis() + 2000);
        Assert.assertEquals(engine.size(), 1);

        engine.cancel(deadlineId);
        Assert.assertEquals(engine.size(), 0);


    }

    @Test
    public void testDeadlineOverdueAndCancel() throws InterruptedException {
        DeadlineEngine engine = new DeadlineEngineImpl();

        long deadlineId = engine.schedule(System.currentTimeMillis() - 2000);

        AtomicInteger a = new AtomicInteger(0);
        engine.poll(System.currentTimeMillis(), (id) -> {
            a.incrementAndGet();
        }, 2);

        Assert.assertEquals(a.get(), 1);

        engine.cancel(deadlineId);
        engine.poll(System.currentTimeMillis(), (id) -> {
            a.incrementAndGet();
        }, 2);
        Assert.assertEquals(a.get(), 1);
        Assert.assertEquals(engine.size(), 0);

    }

    @Test
    public void testDeadlineNotDue() {
        DeadlineEngine engine = new DeadlineEngineImpl();

        long deadlineId = engine.schedule(System.currentTimeMillis() + 2000);
        AtomicInteger a = new AtomicInteger(0);
        engine.poll(System.currentTimeMillis(), (id) -> {
            a.incrementAndGet();
        }, 2);

        Assert.assertEquals(a.get(), 0);
    }

    @Test
    public void testTooManyOverdue(){
        DeadlineEngine engine = new DeadlineEngineImpl();

        long deadlineId = engine.schedule(System.currentTimeMillis() - 2000);
        long deadlineId2 = engine.schedule(System.currentTimeMillis() - 2000);

        AtomicInteger a = new AtomicInteger(0);
        AtomicLong l = new AtomicLong(0);

        engine.poll(System.currentTimeMillis(), (id) -> {
            a.incrementAndGet();
            l.set(id);
        }, 1);
        Assert.assertEquals(a.get(), 1);
        Assert.assertTrue(Long.compare(deadlineId, l.get()) == 0 || Long.compare(deadlineId2, l.get()) == 0);

        engine.poll(System.currentTimeMillis(), (id) -> {
            a.incrementAndGet();
        }, 3);
        Assert.assertEquals(a.get(), 4);


    }

    @Test
    public void testTriggerNumberAndSameTimestamp() {

        DeadlineEngine engine = new DeadlineEngineImpl();
        long ts = System.currentTimeMillis() - 2000;

        long deadlineId = engine.schedule(ts);
        long deadlineId2 = engine.schedule(ts);

        Assert.assertEquals(engine.size(), 2);

        int triggered = engine.poll(System.currentTimeMillis(), (id) -> {
            System.out.println("triggered");
        }, 1);

        Assert.assertEquals(triggered, 1);

        triggered = engine.poll(System.currentTimeMillis(), (id) -> {
            System.out.println("triggered");
        }, 1);

        Assert.assertEquals(triggered, 1);


        triggered = engine.poll(System.currentTimeMillis(), (id) -> {
            System.out.println("triggered");
        }, 4);

        Assert.assertEquals(triggered, 4);
    }

}
