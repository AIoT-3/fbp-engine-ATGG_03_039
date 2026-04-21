package com.fbp.engine.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.impl.CompositeRuleNode;
import com.fbp.engine.node.impl.ModbusWriterNode;
import com.fbp.engine.node.impl.MqttPublisherNode;
import com.fbp.engine.node.impl.MqttSubscriberNode;
import com.fbp.engine.node.impl.PrintNode;
import com.fbp.engine.node.impl.TransformNode;
import com.fbp.engine.protocol.ModbusTcpSimulator;
import java.util.HashMap;
import java.util.Map;
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
@DisplayName("MQTT↔MODBUS 통합 플로우 테스트")
class MqttModbusIntegrationTest {

    private ModbusTcpSimulator simulator;
    private MqttClient testClient;
    private FlowEngine engine;
    private boolean alertReceived;

    private final String BROKER_URL = "tcp://localhost:1883";

    @BeforeEach
    void setUp() throws Exception {
        simulator = new ModbusTcpSimulator(5020, 10);
        simulator.start();
        simulator.setRegister(2, 0);

        testClient = new MqttClient(BROKER_URL, "test-integration-client");
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(true);
        testClient.connect(options);

        alertReceived = false;
        testClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                if ("sensor/alert".equals(topic)) {
                    alertReceived = true;
                }
            }
            @Override public void disconnected(MqttDisconnectResponse response) {}
            @Override public void mqttErrorOccurred(MqttException exception) {}
            @Override public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {}
            @Override public void connectComplete(boolean reconnect, String serverURI) {}
            @Override public void authPacketArrived(int reasonCode, MqttProperties properties) {}
        });
        testClient.subscribe("sensor/alert", 1);

        setupFlow();
        Thread.sleep(1000);
    }

    private void setupFlow() {
        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("topic", "sensor/temperature");
        subConfig.put("clientId", "fbp-e2e-sub");
        subConfig.put("qos", 1);
        MqttSubscriberNode mqttSub = new MqttSubscriberNode("mqtt-sub", subConfig);

        TransformNode parser = new TransformNode("parser", msg -> {
            try {
                byte[] payloadBytes = (byte[]) msg.getPayload().get("payload");
                String rawStr = new String(payloadBytes);
                double temp = Double.parseDouble(rawStr.replaceAll("[^0-9.]", ""));

                Map<String, Object> newPayload = new HashMap<>(msg.getPayload());
                newPayload.put("temperature", temp);
                return new Message(newPayload);
            } catch (Exception e) {
                return msg;
            }
        });

        CompositeRuleNode rule = new CompositeRuleNode("rule", CompositeRuleNode.Operator.AND);
        rule.addCondition("temperature", ">", 30.0);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> payload = new HashMap<>(msg.getPayload());
            payload.put("alertCode", 1);
            payload.put("topic", "sensor/alert");
            return new Message(payload);
        });

        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", 5020);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 2);
        writerConfig.put("valueField", "alertCode");
        ModbusWriterNode writer = new ModbusWriterNode("writer", writerConfig);

        Map<String, Object> pubConfig = new HashMap<>();
        pubConfig.put("brokerUrl", BROKER_URL);
        pubConfig.put("clientId", "fbp-e2e-pub");
        pubConfig.put("topic", "sensor/alert");
        MqttPublisherNode mqttPub = new MqttPublisherNode("mqtt-pub", pubConfig);

        PrintNode printer = new PrintNode("printer");

        Flow flow = new Flow("mqtt-modbus-e2e");

        flow.addNode(mqttSub)
                .addNode(parser)
                .addNode(rule)
                .addNode(mapper)
                .addNode(writer)
                .addNode(mqttPub)
                .addNode(printer)
                .connect(mqttSub.getId(), "out", parser.getId(), "in")
                .connect(parser.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writer.getId(), "in")
                .connect(mapper.getId(), "out", mqttPub.getId(), "in")
                .connect(writer.getId(), "result", printer.getId(), "in");

        engine = new FlowEngine();
        engine.register(flow);
        engine.startFlow(flow.getId());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
        if (simulator != null) simulator.stop();
        if (testClient != null && testClient.isConnected()) {
            testClient.disconnect();
            testClient.close();
        }
    }

    @Test
    @DisplayName("1. MQTT 수신 → Rule 분기")
    void testMqttReceiveAndRuleBranch() throws Exception {
        testClient.publish("sensor/temperature", new MqttMessage("{\"temperature\": 25.0}".getBytes()));
        Thread.sleep(1500);

        assertEquals(0, simulator.getRegister(2));
        assertFalse(alertReceived);
    }

    @Test
    @DisplayName("2. Rule match → MODBUS 쓰기")
    void testRuleMatchToModbusWrite() throws Exception {
        testClient.publish("sensor/temperature", new MqttMessage("{\"temperature\": 35.5}".getBytes()));
        Thread.sleep(1500);

        assertEquals(1, simulator.getRegister(2));
    }

    @Test
    @DisplayName("3. Rule match → MQTT 알림")
    void testRuleMatchToMqttAlert() throws Exception {
        testClient.publish("sensor/temperature", new MqttMessage("{\"temperature\": 38.0}".getBytes()));
        Thread.sleep(1500);

        assertTrue(alertReceived);
    }

    @Test
    @DisplayName("4. End-to-End 흐름")
    void testEndToEndPipeline() throws Exception {
        testClient.publish("sensor/temperature", new MqttMessage("{\"temperature\": 20.0}".getBytes()));
        Thread.sleep(1500);

        assertEquals(0, simulator.getRegister(2));
        assertFalse(alertReceived);

        testClient.publish("sensor/temperature", new MqttMessage("{\"temperature\": 40.0}".getBytes()));
        Thread.sleep(1500);

        assertEquals(1, simulator.getRegister(2));
        assertTrue(alertReceived);
    }
}