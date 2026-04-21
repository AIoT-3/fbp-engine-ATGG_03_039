package com.fbp.engine.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;
import com.fbp.engine.node.impl.MqttPublisherNode;
import com.fbp.engine.node.impl.MqttSubscriberNode;
import com.fbp.engine.node.impl.TransformNode;
import java.lang.reflect.Field;
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
@DisplayName("MQTT 통합 테스트")
class MqttIntegrationTest {

    private static final String BROKER_URL = "tcp://localhost:1883";
    private FlowEngine engine;
    private MqttClient testClient;

    static class TestCollectorNode extends AbstractNode {
        private final List<Message> collected = new ArrayList<>();

        public TestCollectorNode(String id) {
            super(id);
            addInputPort("in");
        }

        @Override
        protected void onProcess(Message message) {
            collected.add(message);
        }

        public List<Message> getCollected() {
            return collected;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        engine = new FlowEngine();

        testClient = new MqttClient(BROKER_URL, "test-integration-client-" + System.currentTimeMillis());
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(true);
        testClient.connect(options);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
        }
        if (testClient != null && testClient.isConnected()) {
            testClient.disconnect();
            testClient.close();
        }
    }

    @Test
    @DisplayName("1. Subscriber → Publisher 파이프라인")
    void testSubscriberToPublisherPipeline() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        boolean[] isReceived = {false};

        testClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                if ("test/out".equals(topic)) {
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
        testClient.subscribe("test/out", 1);

        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", "test/in");
        subConfig.put("clientId", "sub-1-" + System.currentTimeMillis());
        subConfig.put("qos", 1);
        MqttSubscriberNode subNode = new MqttSubscriberNode("sub", subConfig);

        TransformNode transformNode = new TransformNode("transform", msg -> {
            Map<String, Object> newPayload = new HashMap<>(msg.getPayload());
            newPayload.remove("topic");
            return new Message(newPayload);
        });

        Map<String, Object> pubConfig = new HashMap<>();
        pubConfig.put("brokerUrl", BROKER_URL);
        pubConfig.put("topic", "test/out");
        pubConfig.put("clientId", "pub-1-" + System.currentTimeMillis());
        pubConfig.put("qos", 1);
        MqttPublisherNode pubNode = new MqttPublisherNode("pub", pubConfig);

        Flow flow = new Flow("flow-1");
        flow.addNode(subNode).addNode(transformNode).addNode(pubNode)
                .connect(subNode.getId(), "out", transformNode.getId(), "in")
                .connect(transformNode.getId(), "out", pubNode.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1500);

        testClient.publish("test/in", new MqttMessage("{\"data\": 123}".getBytes()));

        boolean result = latch.await(3, TimeUnit.SECONDS);
        assertTrue(result && isReceived[0]);
    }

    @Test
    @DisplayName("2. 다중 토픽 구독")
    void testMultiTopicSubscription() throws Exception {
        String uniquePrefix = "multi_test_" + System.currentTimeMillis();

        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", uniquePrefix + "/+");
        subConfig.put("clientId", "sub-2-" + System.currentTimeMillis());
        subConfig.put("qos", 1);
        MqttSubscriberNode subNode = new MqttSubscriberNode("sub", subConfig);

        TestCollectorNode collector = new TestCollectorNode("collector");

        Flow flow = new Flow("flow-2");
        flow.addNode(subNode).addNode(collector)
                .connect(subNode.getId(), "out", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1500);

        testClient.publish(uniquePrefix + "/temp", new MqttMessage("{\"value\": 20}".getBytes()));
        testClient.publish(uniquePrefix + "/humidity", new MqttMessage("{\"value\": 50}".getBytes()));
        Thread.sleep(2000);

        assertEquals(2, collector.getCollected().size());
    }

    @Test
    @DisplayName("3. QoS 1 전달 보장")
    void testQos1DeliveryGuarantee() throws Exception {
        String uniqueTopic = "qos1/test/" + System.currentTimeMillis();

        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", uniqueTopic);
        subConfig.put("clientId", "sub-3-" + System.currentTimeMillis());
        subConfig.put("qos", 1);
        MqttSubscriberNode subNode = new MqttSubscriberNode("sub", subConfig);

        TestCollectorNode collector = new TestCollectorNode("collector");

        Flow flow = new Flow("flow-3");
        flow.addNode(subNode).addNode(collector)
                .connect(subNode.getId(), "out", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1500);

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            MqttMessage msg = new MqttMessage(("{\"id\": " + i + "}").getBytes());
            msg.setQos(1);
            testClient.publish(uniqueTopic, msg);
        }
        Thread.sleep(2000);

        assertEquals(messageCount, collector.getCollected().size());
    }

    @Test
    @DisplayName("4. 재연결 테스트 (설정 검증)")
    void testReconnection() throws Exception {
        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", "reconnect/test");
        subConfig.put("clientId", "sub-4-" + System.currentTimeMillis());
        subConfig.put("qos", 1);
        MqttSubscriberNode subNode = new MqttSubscriberNode("sub", subConfig);

        TestCollectorNode collector = new TestCollectorNode("collector");

        Flow flow = new Flow("flow-4");
        flow.addNode(subNode).addNode(collector)
                .connect(subNode.getId(), "out", collector.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1500);

        Field clientField = MqttSubscriberNode.class.getDeclaredField("client");
        clientField.setAccessible(true);
        MqttClient internalClient = (MqttClient) clientField.get(subNode);

        assertTrue(internalClient.isConnected());

        testClient.publish("reconnect/test", new MqttMessage("{\"status\": \"ok\"}".getBytes()));
        Thread.sleep(2000);

        assertEquals(1, collector.getCollected().size());
    }
}