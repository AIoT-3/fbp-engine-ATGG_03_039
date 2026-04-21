package com.fbp.engine.runner;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.node.impl.GeneratorNode;
import com.fbp.engine.node.impl.MqttPublisherNode;
import java.util.HashMap;
import java.util.Map;

public class MqttPublisherMain {


    public static void main(String[] args) throws InterruptedException {
        Flow flow = new Flow("flow");
        FlowEngine engine = new FlowEngine();

        GeneratorNode generatorNode = new GeneratorNode("generator");

        String brokerUrl = "tcp://localhost:1883"; // Docker로 띄운 로컬 모스퀴토
        String clientId = "paho-test-client";      // 임의의 클라이언트 ID
        String testTopic = "sensor/generated";          // 토픽


        Map<String, Object> pubConfig = new HashMap<>();
        pubConfig.put("brokerUrl", brokerUrl);
        pubConfig.put("clientId", clientId);
        pubConfig.put("topic", testTopic);
        pubConfig.put("qos", 1);
        pubConfig.put("retained",true);

        MqttPublisherNode publisherNode = new MqttPublisherNode("mqttPub", pubConfig);

        flow.addNode(generatorNode)
                .addNode(publisherNode)
                .connect(generatorNode.getId(), "out", publisherNode.getId(), "in");
        engine.register(flow);
        engine.startFlow(flow.getId());

        Thread.sleep(1000);

        for(int i = 1 ; i<=10; i++){
            double tempValue = Math.round((20.0 + Math.random() * 10) * 10) / 10.0;
            generatorNode.generate("temperature", tempValue);
            Thread.sleep(2000);
        }
        engine.shutdown();
    }
}
