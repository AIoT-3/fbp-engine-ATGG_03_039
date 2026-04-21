package com.fbp.engine.runner;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.impl.CompositeRuleNode;
import com.fbp.engine.node.impl.ModbusWriterNode;
import com.fbp.engine.node.impl.MqttSubscriberNode;
import com.fbp.engine.node.impl.PrintNode;
import com.fbp.engine.node.impl.TransformNode;
import com.fbp.engine.protocol.ModbusTcpSimulator;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;

@Slf4j
public class Step4_5_Main {
    public static void main(String[] args) throws MqttException, InterruptedException {
        ModbusTcpSimulator simulator = new ModbusTcpSimulator(5020, 10);
        simulator.start();
        simulator.setRegister(2,0);

        String brokerUrl = "tcp://localhost:1883";
        MqttClient testPublisher = new MqttClient(brokerUrl, "test-publisher");
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(true);
        testPublisher.connect(options);

        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", brokerUrl);
        subConfig.put("topic", "sensor/temperature");
        subConfig.put("clientId", "fbp-e2e-sub");
        subConfig.put("qos",1);

        MqttSubscriberNode mqttSub = new MqttSubscriberNode("mqtt-sub", subConfig);

        TransformNode parser = new TransformNode("parser", msg -> {
            try {
                // 수신된 페이로드 바이트 배열을 문자열로 변환 후 숫자만 추출
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
            return new Message(payload);
        });
        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", 5020);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 2);
        writerConfig.put("valueField", "alertCode");
        ModbusWriterNode writer = new ModbusWriterNode("writer", writerConfig);
        PrintNode printer = new PrintNode("printer");

        Flow flow = new Flow("mqtt-modbus-e2e");

        flow.addNode(mqttSub)
                .addNode(parser)
                .addNode(rule)
                .addNode(mapper)
                .addNode(writer)
                .addNode(printer)
                .connect(mqttSub.getId(), "out", parser.getId(),"in")
                .connect(parser.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match",mapper.getId(),"in")
                .connect(mapper.getId(), "out", writer.getId(),"in")
                .connect(writer.getId(), "result", printer.getId(), "in");

        FlowEngine engine = new FlowEngine();
        engine.register(flow);
        engine.startFlow(flow.getId());
        Thread.sleep(1500);

        log.info("--- 정상 온도 25.0 발행 (알람 X) ---");
        testPublisher.publish("sensor/temperature", new MqttMessage("{\"temperature\": 25.0}".getBytes()));
        Thread.sleep(2000);

        log.info("--- 고온 35.5 발행 (알람 조건 충족!) ---");
        testPublisher.publish("sensor/temperature", new MqttMessage("{\"temperature\": 35.5}".getBytes()));
        Thread.sleep(2000);

        int finalAlertStatus = simulator.getRegister(2);
        log.info("--------------------------------------------------");
        if (finalAlertStatus == 1) {
            log.info("성공: MQTT 수신 -> Rule 분기 -> MODBUS 레지스터 제어 완벽 동작");
        } else {
            log.error("실패: 알람 레지스터가 변경되지 않았습니다.");
        }
        log.info("--------------------------------------------------");

        engine.shutdown();
        simulator.stop();
        testPublisher.disconnect();
        testPublisher.close();
    }
}
