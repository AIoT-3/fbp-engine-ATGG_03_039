package com.fbp.engine.runner;

import com.fbp.engine.core.Connection;
import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Step5_3_PerformanceTest {

    static class LoadGeneratorNode extends AbstractNode {
        public LoadGeneratorNode(String id) {
            super(id);
            addOutputPort("out");
        }

        @Override
        protected void onProcess(Message message) { }

        public void generateNMessages(int count) {
            for (int i = 0; i < count; i++) {
                Map<String, Object> payload = new HashMap<>();
                payload.put("dataId", i);
                payload.put("startTime", System.nanoTime());
                send("out", new Message(payload));
            }
        }
    }

    static class DummyProcessNode extends AbstractNode {
        public DummyProcessNode(String id) {
            super(id);
            addInputPort("in");
            addOutputPort("out");
        }

        @Override
        protected void onProcess(Message message) {
            String data = "Processing Data: " + message.getPayload().get("dataId");
            data = data.toUpperCase();
            send("out", message);
        }
    }

    static class MetricsCollectorNode extends AbstractNode {
        private final CountDownLatch latch;
        private final int targetCount;
        private int currentCount = 0;
        private long totalLatencyNano = 0;

        public MetricsCollectorNode(String id, int targetCount, CountDownLatch latch) {
            super(id);
            this.targetCount = targetCount;
            this.latch = latch;
            addInputPort("in");
        }

        @Override
        protected void onProcess(Message message) {
            long endTime = System.nanoTime();
            long startTime = (Long) message.getPayload().get("startTime");
            
            totalLatencyNano += (endTime - startTime);
            currentCount++;

            if (currentCount == targetCount) {
                latch.countDown();
            }
        }

        public double getAverageLatencyMs() {
            return (totalLatencyNano / (double) targetCount) / 1_000_000.0;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int[] testCases = {100, 500, 1000};

        log.info("성능 측정 벤치마크를 시작합니다...\n");

        for (int msgCount : testCases) {
            runBenchmark(msgCount);
            Thread.sleep(1000);
        }
    }

    private static void runBenchmark(int messageCount) throws InterruptedException {
        System.gc();
        Thread.sleep(500);
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();

        FlowEngine engine = new FlowEngine();
        Flow flow = new Flow("benchmark-flow");

        CountDownLatch latch = new CountDownLatch(1);

        LoadGeneratorNode generator = new LoadGeneratorNode("generator");
        DummyProcessNode processor1 = new DummyProcessNode("processor-1");
        DummyProcessNode processor2 = new DummyProcessNode("processor-2");
        MetricsCollectorNode collector = new MetricsCollectorNode("collector", messageCount, latch);

        flow.addNode(generator).addNode(processor1).addNode(processor2).addNode(collector)
            .connect(generator.getId(), "out", processor1.getId(), "in")
            .connect(processor1.getId(), "out", processor2.getId(), "in")
            .connect(processor2.getId(), "out", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        long testStartNano = System.nanoTime();

        generator.generateNMessages(messageCount);

        int maxBacklog = 0;
        for (Connection conn : flow.getConnections()) {
            maxBacklog += conn.getBufferSize();
        }

        latch.await();

        long testEndNano = System.nanoTime();
        double elapsedSec = (testEndNano - testStartNano) / 1_000_000_000.0;

        double throughput = messageCount / elapsedSec;

        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        double usedMemoryMb = (finalMemory - initialMemory) / (1024.0 * 1024.0);

        log.info("==================================================");
        log.info("테스트 메시지 수 : {} msg", messageCount);
        log.info("총 소요 시간    : {} 초\n", elapsedSec);
        log.info("Throughput      : {} msg/sec\n", throughput);
        log.info("Avg Latency     : {}} ms\n", collector.getAverageLatencyMs());
        log.info("Queue 적체량    : {}} msgs (최대 피크)\n", maxBacklog);
        log.info("메모리 사용량   : {}} MB\n", usedMemoryMb < 0 ? 0 : usedMemoryMb);
        log.info("==================================================\n");

        engine.shutdown();
    }
}