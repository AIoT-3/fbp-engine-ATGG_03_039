package com.fbp.engine.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;
import com.fbp.engine.node.impl.CompositeRuleNode;
import com.fbp.engine.node.impl.ModbusReaderNode;
import com.fbp.engine.node.impl.ModbusWriterNode;
import com.fbp.engine.node.impl.MqttPublisherNode;
import com.fbp.engine.node.impl.MqttSubscriberNode;
import com.fbp.engine.node.impl.TimerNode;
import com.fbp.engine.node.impl.TransformNode;
import com.fbp.engine.protocol.ModbusTcpSimulator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
@DisplayName("크로스 프로토콜 통합 테스트")
class CrossProtocolIntegrationTest {

    private static final String BROKER_URL = "tcp://localhost:1883";
    private FlowEngine engine;
    private ModbusTcpSimulator simulator;
    private MqttClient testClient;
    private static int portCounter = 26020;
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
    void setUp() throws Exception {
        engine = new FlowEngine();

        currentPort = portCounter++;
        simulator = new ModbusTcpSimulator(currentPort, 10);
        simulator.start();
        Thread.sleep(200);

        simulator.setRegister(0, 0);
        simulator.setRegister(2, 0);

        testClient = new MqttClient(BROKER_URL, "cross-test-client-" + System.currentTimeMillis());
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(true);
        testClient.connect(options);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
        }
        if (simulator != null) {
            simulator.stop();
        }
        if (testClient != null && testClient.isConnected()) {
            testClient.disconnect();
            testClient.close();
        }
    }

    @Test
    @DisplayName("1. MQTT → Rule → MODBUS")
    void testMqttToRuleToModbus() throws Exception {
        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", "cross/in");
        subConfig.put("clientId", "sub-cross-1");
        subConfig.put("qos", 1);
        MqttSubscriberNode subNode = new MqttSubscriberNode("sub", subConfig);


        CompositeRuleNode rule = new CompositeRuleNode("rule", CompositeRuleNode.Operator.AND);
        rule.addCondition("temperature", ">", 30.0);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> p = new HashMap<>();
            p.put("alertCode", 1);
            return new Message(p);
        });

        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", currentPort);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 2);
        writerConfig.put("valueField", "alertCode");
        ModbusWriterNode writer = new ModbusWriterNode("writer", writerConfig);

        CountDownLatch latch = new CountDownLatch(1);
        TestCollectorNode collector = new TestCollectorNode("collector");
        collector.setLatch(latch);

        Flow flow = new Flow("flow-cross-1");
        flow.addNode(subNode).addNode(rule).addNode(mapper).addNode(writer).addNode(collector)
                .connect(subNode.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writer.getId(), "in")
                .connect(writer.getId(), "result", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1000);

        testClient.publish("cross/in", new MqttMessage("{\"temperature\": 35.5}".getBytes()));

        assertTrue(latch.await(4, TimeUnit.SECONDS));
        assertEquals(1, simulator.getRegister(2));
    }

    @Test
    @DisplayName("2. MODBUS → Rule → MQTT")
    void testModbusToRuleToMqtt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        boolean[] isReceived = {false};

        testClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                if ("cross/out".equals(topic)) {
                    isReceived[0] = true;
                    latch.countDown();
                }
            }
            @Override public void disconnected(MqttDisconnectResponse r) {}
            @Override public void mqttErrorOccurred(MqttException e) {}
            @Override public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken t) {}
            @Override public void connectComplete(boolean r, String s) {}
            @Override public void authPacketArrived(int r, MqttProperties p) {}
        });
        testClient.subscribe("cross/out", 1);

        TimerNode timer = new TimerNode("timer", 500);

        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("host", "localhost");
        readerConfig.put("port", currentPort);
        readerConfig.put("slaveId", 1);
        readerConfig.put("startAddress", 0);
        readerConfig.put("count", 1);
        readerConfig.put("registerMapping", Map.of("0", Map.of("name", "temperature", "scale", 1.0)));
        ModbusReaderNode reader = new ModbusReaderNode("reader", readerConfig);

        CompositeRuleNode rule = new CompositeRuleNode("rule", CompositeRuleNode.Operator.AND);
        rule.addCondition("temperature", ">", 30.0);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> p = new HashMap<>(msg.getPayload());
            p.put("topic", "cross/out");
            return new Message(p);
        });

        Map<String, Object> pubConfig = new HashMap<>();
        pubConfig.put("brokerUrl", BROKER_URL);
        pubConfig.put("topic", "cross/out");
        pubConfig.put("clientId", "pub-cross-2");
        pubConfig.put("qos", 1);
        MqttPublisherNode pubNode = new MqttPublisherNode("pub", pubConfig);

        Flow flow = new Flow("flow-cross-2");
        flow.addNode(timer).addNode(reader).addNode(rule).addNode(mapper).addNode(pubNode)
                .connect(timer.getId(), "out", reader.getId(), "trigger")
                .connect(reader.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", pubNode.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        Thread.sleep(1000);
        simulator.setRegister(0, 45);

        assertTrue(latch.await(4, TimeUnit.SECONDS));
        assertTrue(isReceived[0]);
    }

    @Test
    @DisplayName("3. 복합 플로우 안정성")
    void testComplexFlowStability() throws Exception {
        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", "stability/in");
        subConfig.put("clientId", "sub-stab-3");
        subConfig.put("qos", 1);
        MqttSubscriberNode subNode = new MqttSubscriberNode("sub", subConfig);

        CompositeRuleNode rule = new CompositeRuleNode("rule", CompositeRuleNode.Operator.AND);
        rule.addCondition("temperature", ">", 30.0);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> p = new HashMap<>();
            p.put("alertCode", 1);
            return new Message(p);
        });

        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", currentPort);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 2);
        writerConfig.put("valueField", "alertCode");
        ModbusWriterNode writer = new ModbusWriterNode("writer", writerConfig);

        CountDownLatch latch = new CountDownLatch(10);
        TestCollectorNode collector = new TestCollectorNode("collector");
        collector.setLatch(latch);

        Flow flow = new Flow("flow-stab-3");
        flow.addNode(subNode).addNode(rule).addNode(mapper).addNode(writer).addNode(collector)
                .connect(subNode.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writer.getId(), "in")
                .connect(writer.getId(), "result", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1000);

        long endTime = System.currentTimeMillis() + 5000;
        int count = 0;
        while (System.currentTimeMillis() < endTime) {
            testClient.publish("stability/in", new MqttMessage("{\"temperature\": 35.5}".getBytes()));
            count++;
            Thread.sleep(100);
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(collector.getCollected().size() >= 10);
        assertEquals(1, simulator.getRegister(2));
    }
}