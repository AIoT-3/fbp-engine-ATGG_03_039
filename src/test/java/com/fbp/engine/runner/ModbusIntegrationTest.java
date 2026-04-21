package com.fbp.engine.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fbp.engine.core.Connection;
import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;
import com.fbp.engine.node.impl.ModbusReaderNode;
import com.fbp.engine.node.impl.ModbusWriterNode;
import com.fbp.engine.node.impl.TimerNode;
import com.fbp.engine.node.impl.TransformNode;
import com.fbp.engine.protocol.ModbusTcpSimulator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
@DisplayName("MODBUS 통합 테스트")
class ModbusIntegrationTest {

    private ModbusTcpSimulator simulator;
    private FlowEngine engine;
    private static int portCounter = 25020;
    private int currentPort;

    static class TestCollectorNode extends AbstractNode {
        private final List<Message> collected = new ArrayList<>();
        private CountDownLatch latch;

        public TestCollectorNode(String id) {
            super(id);
            addInputPort("in");
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        protected void onProcess(Message message) {
            collected.add(message);
            if (latch != null) {
                latch.countDown();
            }
        }

        public List<Message> getCollected() {
            return collected;
        }
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        currentPort = portCounter++;
        simulator = new ModbusTcpSimulator(currentPort, 10);
        simulator.start();
        Thread.sleep(200);

        simulator.setRegister(0, 0);
        simulator.setRegister(2, 0);
        engine = new FlowEngine();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
        if (simulator != null) {
            simulator.stop();
        }
    }

    @Test
    @DisplayName("1. Reader → 레지스터 읽기")
    void testReaderToRegister() throws Exception {
        simulator.setRegister(0, 1234);

        TimerNode timer = new TimerNode("timer", 500);

        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("host", "localhost");
        readerConfig.put("port", currentPort);
        readerConfig.put("slaveId", 1);
        readerConfig.put("startAddress", 0);
        readerConfig.put("count", 1);
        readerConfig.put("registerMapping", Map.of("0", Map.of("name", "sensorValue", "scale", 1.0)));
        ModbusReaderNode reader = new ModbusReaderNode("reader", readerConfig);

        CountDownLatch latch = new CountDownLatch(1);
        TestCollectorNode collector = new TestCollectorNode("collector");
        collector.setLatch(latch);

        Flow flow = new Flow("flow-1");
        flow.addNode(timer).addNode(reader).addNode(collector)
                .connect(timer.getId(), "out", reader.getId(), "trigger")
                .connect(reader.getId(), "out", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        assertTrue(latch.await(3, TimeUnit.SECONDS));

        Message msg = collector.getCollected().get(0);
        double value = (Double) msg.getPayload().get("sensorValue");
        assertEquals(1234.0, value);
    }

    @Test
    @DisplayName("2. Writer → 레지스터 쓰기")
    void testWriterToRegister() throws Exception {
        TimerNode timer = new TimerNode("timer", 500);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> payload = new HashMap<>();
            payload.put("command", 5678);
            return new Message(payload);
        });

        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", currentPort);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 2);
        writerConfig.put("valueField", "command");
        ModbusWriterNode writer = new ModbusWriterNode("writer", writerConfig);

        CountDownLatch latch = new CountDownLatch(1);
        TestCollectorNode collector = new TestCollectorNode("collector");
        collector.setLatch(latch);

        Flow flow = new Flow("flow-2");
        flow.addNode(timer).addNode(mapper).addNode(writer).addNode(collector)
                .connect(timer.getId(), "out", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writer.getId(), "in")
                .connect(writer.getId(), "result", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(5678, simulator.getRegister(2));
    }

    @Test
    @DisplayName("3. Reader → Writer 파이프라인")
    void testReaderToWriterPipeline() throws Exception {
        simulator.setRegister(0, 9999);

        TimerNode timer = new TimerNode("timer", 500);

        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("host", "localhost");
        readerConfig.put("port", currentPort);
        readerConfig.put("slaveId", 1);
        readerConfig.put("startAddress", 0);
        readerConfig.put("count", 1);
        readerConfig.put("registerMapping", Map.of("0", Map.of("name", "sensorValue", "scale", 1.0)));
        ModbusReaderNode reader = new ModbusReaderNode("reader", readerConfig);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> payload = new HashMap<>(msg.getPayload());
            double val = (Double) payload.get("sensorValue");
            payload.put("command", (int) val);
            return new Message(payload);
        });

        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", currentPort);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 2);
        writerConfig.put("valueField", "command");
        ModbusWriterNode writer = new ModbusWriterNode("writer", writerConfig);

        CountDownLatch latch = new CountDownLatch(1);
        TestCollectorNode collector = new TestCollectorNode("collector");
        collector.setLatch(latch);

        Flow flow = new Flow("flow-3");
        flow.addNode(timer).addNode(reader).addNode(mapper).addNode(writer).addNode(collector)
                .connect(timer.getId(), "out", reader.getId(), "trigger")
                .connect(reader.getId(), "out", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writer.getId(), "in")
                .connect(writer.getId(), "result", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        assertTrue(latch.await(4, TimeUnit.SECONDS));
        assertEquals(9999, simulator.getRegister(2));
    }

    @Test
    @DisplayName("4. 연결 끊김 처리")
    void testConnectionDropHandling() throws Exception {
        TimerNode timer = new TimerNode("timer", 500);

        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("host", "localhost");
        readerConfig.put("port", currentPort);
        readerConfig.put("slaveId", 1);
        readerConfig.put("startAddress", 0);
        readerConfig.put("count", 1);
        readerConfig.put("registerMapping", Map.of("0", Map.of("name", "sensorValue", "scale", 1.0)));
        ModbusReaderNode reader = new ModbusReaderNode("reader", readerConfig);

        CountDownLatch latch = new CountDownLatch(1);
        TestCollectorNode errorCollector = new TestCollectorNode("error-collector");
        errorCollector.setLatch(latch);

        Flow flow = new Flow("flow-4");
        flow.addNode(timer).addNode(reader).addNode(errorCollector)
                .connect(timer.getId(), "out", reader.getId(), "trigger");

        Connection errorConn = new Connection("error-conn");
        errorConn.setTarget(errorCollector.getInputPort("in"));
        reader.getOutputPort("error").connect(errorConn);
        flow.getConnections().add(errorConn);

        engine.register(flow);
        engine.startFlow(flow.getId());

        Thread.sleep(1000);
        simulator.stop();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(errorCollector.getCollected().size() > 0);
        assertTrue(errorCollector.getCollected().get(0).getPayload().containsKey("error"));
    }
}