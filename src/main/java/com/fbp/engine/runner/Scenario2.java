package com.fbp.engine.runner;

import com.fbp.engine.core.Flow;
import com.fbp.engine.core.FlowEngine;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.impl.CompositeRuleNode;
import com.fbp.engine.node.impl.CompositeRuleNode.Operator;
import com.fbp.engine.node.impl.ModbusReaderNode;
import com.fbp.engine.node.impl.ModbusWriterNode;
import com.fbp.engine.node.impl.PrintNode;
import com.fbp.engine.node.impl.TimerNode;
import com.fbp.engine.node.impl.TransformNode;
import com.fbp.engine.protocol.ModbusTcpSimulator;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Scenario2 {

    public static void main(String[] args) throws InterruptedException {
        ModbusTcpSimulator simulator = new ModbusTcpSimulator(5020, 10);
        simulator.start();
        simulator.setRegister(0,0);
        simulator.setRegister(2,0);

        FlowEngine engine = new FlowEngine();
        Flow flow = new Flow("scenario-2");

        TimerNode timerNode = new TimerNode("timer", 1000);

        Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("host", "localhost");
        readerConfig.put("port", 5020);
        readerConfig.put("slaveId", 1);
        readerConfig.put("startAddress", 0);
        readerConfig.put("count", 1);
        readerConfig.put("registerMapping", Map.of("0", Map.of("name", "temperature", "scale", 0.1)));
        ModbusReaderNode readerNode = new ModbusReaderNode("modbus-reader", readerConfig);

        CompositeRuleNode rule = new CompositeRuleNode("rule", Operator.AND);
        rule.addCondition("temperature", ">", 30.0);

        TransformNode mapper = new TransformNode("mapper", msg->{
            Map<String, Object> p = new HashMap<>(msg.getPayload());
            p.put("alertCode", 1);
            return new Message(p);
        });

        Map<String, Object> writerconfig = new HashMap<>();
        writerconfig.put("host", "localhost");
        writerconfig.put("port", 5020);
        writerconfig.put("slaveId", 1);
        writerconfig.put("registerAddress", 2);
        writerconfig.put("valueField", "alertCode");
        ModbusWriterNode writerNode = new ModbusWriterNode("modbus-writer", writerconfig);
        PrintNode printer = new PrintNode("printer");

        flow.addNode(timerNode)
                .addNode(readerNode)
                .addNode(rule)
                .addNode(mapper)
                .addNode(writerNode)
                .addNode(printer)
                .connect(timerNode.getId(), "out", readerNode.getId(),"trigger")
                .connect(readerNode.getId(),"out", rule.getId(), "in")
                .connect(rule.getId(), "match", mapper.getId(), "in")
                .connect(mapper.getId(), "out", writerNode.getId(), "in")
                .connect(writerNode.getId(),"result", printer.getId(), "in");

        engine.register(flow);
        engine.startFlow(flow.getId());

        Thread.sleep(2000);

        simulator.setRegister(0, 350);

        Thread.sleep(3000);
        if(simulator.getRegister(2)==1){
            log.info("시나리오2 성공");
        }else{
            log.error("시나리오2 실패");
        }
        engine.shutdown();
        simulator.stop();
    }
}
