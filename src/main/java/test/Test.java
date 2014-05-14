package test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestRunner;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Test extends AbstractTest {

    private final static ILogger log = Logger.getLogger(Test.class);

    private IAtomicLong totalCounter;
    private AtomicLong operations = new AtomicLong();
    private IAtomicLong counter;

    //props
    public int threadCount = 1;
    public int itemCount = 1500000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(getTestId() + ":TotalCounter");
        counter = targetInstance.getAtomicLong("counter");
    }

    @Override
    public void createTestThreads() {

        long chunk = itemCount / threadCount;

        for (int k = 0; k < threadCount; k++) {
            spawn(new TestWorker(0, k * chunk, chunk));
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long foundCount = counter.get();

        if (expectedCount != foundCount) {
//            throw new TestFailureException("Expected count: " + expectedCount + " but found count was: " + foundCount);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        counter.destroy();
        totalCounter.destroy();
    }

    @Override
    public long getOperationCount() {
        return operations.get();
    }

    private class TestWorker implements Runnable {

        private long baseId;
        private long firstItem;
        private long itemCount;
        private IMap<String, Long> map;
        private HazelcastInstance targetInstance;

        private TestWorker(final long baseId, final long firstItem, final long itemCount) {
            this.baseId = baseId;
            this.firstItem = firstItem;
            this.itemCount = itemCount;
        }

        private void generateData() {

            for (long i = 0; i < itemCount; i++) {

                if (stopped()) {
                    return;
                }

                long itemId = baseId + firstItem + i;
                final String key = "" + itemId;

                map.executeOnKey(key, new IncrementEntryProcessor(1));
            }
        }

        @Override
        public void run() {

            targetInstance = getTargetInstance();

            map = targetInstance.getMap("testmap");

            log.info(Thread.currentThread().getName() + " generating data...");

            generateData();

            log.info(Thread.currentThread().getName() + " worker finished");
        }
    }

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Long> {
        private final long increment;

        private IncrementEntryProcessor(long increment) {
            this.increment = increment;
        }

        @Override
        public Object process(Map.Entry<Integer, Long> entry) {
            Long value = entry.getValue();
            if (value == null) {
                value = 1L;
            } else {
                value = value + increment;
            }
            entry.setValue(value);
            return value;
        }
    }

    public static void main(String[] args) throws Exception {
        Test test = new Test();
        new TestRunner().run(test, 720);
    }
}

