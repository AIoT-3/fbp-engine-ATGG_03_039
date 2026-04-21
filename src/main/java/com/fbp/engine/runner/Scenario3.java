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

@Slf4j
public class Scenario3 {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC = "sensor/cross";

    public static void main(String[] args) throws Exception {
        ModbusTcpSimulator simulator = new ModbusTcpSimulator(5020, 10);
        simulator.start();
        simulator.setRegister(3, 0);

        FlowEngine engine = new FlowEngine();
        Flow flow = new Flow("scenario-3");

        Map<String, Object> subConfig = new HashMap<>();
        subConfig.put("brokerUrl", BROKER_URL);
        subConfig.put("clientId", "sc3-sub-node");
        subConfig.put("topic", TOPIC);
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
            p.put("commandCode", 99);
            return new Message(p);
        });

        Map<String, Object> writerConfig = new HashMap<>();
        writerConfig.put("host", "localhost");
        writerConfig.put("port", 5020);
        writerConfig.put("slaveId", 1);
        writerConfig.put("registerAddress", 3);
        writerConfig.put("valueField", "commandCode");
        ModbusWriterNode writer = new ModbusWriterNode("modbus-writer", writerConfig);

        PrintNode printer = new PrintNode("printer");

        flow.addNode(mqttSub).addNode(parser).addNode(rule).addNode(mapper).addNode(writer).addNode(printer)
                .connect(mqttSub.getId(), "out", parser.getId(), "in")
                .connect(parser.getId(), "out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writer.getId(), "in")
                .connect(writer.getId(), "result", printer.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        log.info("터미널에서 아래 명령어로 데이터를 보내주세요.");

        Thread.currentThread().join();
    }
}