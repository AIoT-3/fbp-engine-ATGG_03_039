package com.fbp.engine.runner;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.impl.CompositeRuleNode;
import com.fbp.engine.node.impl.MqttPublisherNode;
import com.fbp.engine.node.impl.MqttSubscriberNode;
import com.fbp.engine.node.impl.PrintNode;
import com.fbp.engine.node.impl.TransformNode;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Scenario1 {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String SUB_TOPIC = "sensor/temp";
    private static final String PUB_TOPIC = "alert/temp";

    public static void main(String[] args) throws Exception {
        FlowEngine engine = new FlowEngine();
        Flow flow = new Flow("scenario-1");

        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("clientId", "sc1-sub-node");
        subConfig.put("topic", SUB_TOPIC);
        subConfig.put("qos", 1);
        MqttSubscriberNode mqttSub = new MqttSubscriberNode("mqtt-sub", subConfig);

        TransformNode parser = new TransformNode("parser", msg -> {
            try {
                byte[] bytes = (byte[]) msg.getPayload().get("payload");
                double val = Double.parseDouble(new String(bytes).replaceAll("[^0-9.]", ""));
                Map<String, Object> p = new HashMap<>(msg.getPayload());
                p.put("temperature", val);
                return new Message(p);
            } catch (Exception e) { return msg; }
        });

        CompositeRuleNode rule = new CompositeRuleNode("rule", CompositeRuleNode.Operator.AND);
        rule.addCondition("temperature", ">", 30.0);

        TransformNode mapper = new TransformNode("mapper", msg -> {
            Map<String, Object> p = new HashMap<>(msg.getPayload());
            p.put("topic", PUB_TOPIC);
            p.put("alertMessage", "온도 초과 경고!");
            return new Message(p);
        });

        Map<String, Object> pubConfig = new HashMap<>();
        pubConfig.put("brokerUrl", BROKER_URL);
        pubConfig.put("clientId", "sc1-pub-node");
        pubConfig.put("topic", PUB_TOPIC);
        pubConfig.put("qos", 1);
        MqttPublisherNode mqttPub = new MqttPublisherNode("mqtt-pub", pubConfig);
        PrintNode printer = new PrintNode("printer");

        flow.addNode(mqttSub).addNode(parser).addNode(rule).addNode(mapper).addNode(mqttPub).addNode(printer)
                .connect(mqttSub.getId(), "out", parser.getId(), "in")
                .connect(parser.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", mqttPub.getId(), "in")
                .connect(mapper.getId(), "out", printer.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        log.info("터미널에서 mosquitto_pub 명령어로 데이터를 보내주세요...");

        Thread.currentThread().join();
    }
}