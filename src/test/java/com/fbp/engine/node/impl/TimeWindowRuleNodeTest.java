package com.fbp.engine.node.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fbp.engine.core.Connection;
import com.fbp.engine.message.Message;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("TimeWindowRuleNode 테스트")
class TimeWindowRuleNodeTest {

    private CollectorNode alertCollector;
    private CollectorNode passCollector;
    private Connection alertConnection;
    private Connection passConnection;

    @BeforeEach
    void setUp() {
        alertCollector = new CollectorNode("alert-collector");
        passCollector = new CollectorNode("pass-collector");

        alertConnection = new Connection("toAlert");
        alertConnection.setTarget(alertCollector.getInputPort("in"));

        passConnection = new Connection("toPass");
        passConnection.setTarget(passCollector.getInputPort("in"));
    }

    private void processAndCollect(TimeWindowRuleNode node, Message msg) {
        node.process(msg);

        if (alertConnection.getBufferSize() > 0) {
            Message alertMsg = alertConnection.poll();
            if (alertMsg != null) {
                alertCollector.process(alertMsg);
            }
        }

        if (passConnection.getBufferSize() > 0) {
            Message passMsg = passConnection.poll();
            if (passMsg != null) {
                passCollector.process(passMsg);
            }
        }
    }

    @Test
    @DisplayName("1. 기준 미달 → pass")
    void testConditionMetBelowThreshold() {
        TimeWindowRuleNode node = new TimeWindowRuleNode("time-rule",
                msg -> {
                    Double temp = (Double) msg.getPayload().get("temperature");
                    return temp != null && temp > 30.0;
                },
                1000, 3);

        node.getOutputPort("alert").connect(alertConnection);
        node.getOutputPort("pass").connect(passConnection);

        Message highTemp = new Message(Map.of("temperature", 35.0));

        processAndCollect(node, highTemp);
        assertEquals(0, alertCollector.getCollected().size());
        assertEquals(1, passCollector.getCollected().size());

        processAndCollect(node, highTemp);
        assertEquals(0, alertCollector.getCollected().size());
        assertEquals(2, passCollector.getCollected().size());
    }

    @Test
    @DisplayName("2. 기준 도달 → alert")
    void testThresholdReached() {
        TimeWindowRuleNode node = new TimeWindowRuleNode("time-rule",
                msg -> {
                    Double temp = (Double) msg.getPayload().get("temperature");
                    return temp != null && temp > 30.0;
                },
                1000, 3);

        node.getOutputPort("alert").connect(alertConnection);
        node.getOutputPort("pass").connect(passConnection);

        Message highTemp = new Message(Map.of("temperature", 35.0));

        processAndCollect(node, highTemp);
        processAndCollect(node, highTemp);
        processAndCollect(node, highTemp);

        assertEquals(1, alertCollector.getCollected().size());
        assertEquals(2, passCollector.getCollected().size());
    }

    @Test
    @DisplayName("3. 시간 창 만료")
    void testEventExpiration() throws InterruptedException {
        TimeWindowRuleNode node = new TimeWindowRuleNode("time-rule",
                msg -> {
                    Double temp = (Double) msg.getPayload().get("temperature");
                    return temp != null && temp > 30.0;
                },
                500, 2);

        node.getOutputPort("alert").connect(alertConnection);
        node.getOutputPort("pass").connect(passConnection);

        Message highTemp = new Message(Map.of("temperature", 35.0));

        processAndCollect(node, highTemp);
        assertEquals(0, alertCollector.getCollected().size());
        assertEquals(1, passCollector.getCollected().size());

        Thread.sleep(600);

        processAndCollect(node, highTemp);
        assertEquals(0, alertCollector.getCollected().size());
        assertEquals(2, passCollector.getCollected().size());

        processAndCollect(node, highTemp);
        assertEquals(1, alertCollector.getCollected().size());
        assertEquals(2, passCollector.getCollected().size());
    }

    @Test
    @DisplayName("4. 조건 불만족 메시지")
    void testConditionNotMet() {
        TimeWindowRuleNode node = new TimeWindowRuleNode("time-rule",
                msg -> {
                    Double temp = (Double) msg.getPayload().get("temperature");
                    return temp != null && temp > 30.0;
                },
                1000, 2);

        node.getOutputPort("alert").connect(alertConnection);
        node.getOutputPort("pass").connect(passConnection);

        Message lowTemp = new Message(Map.of("temperature", 25.0));
        Message highTemp = new Message(Map.of("temperature", 35.0));

        processAndCollect(node, lowTemp);
        assertEquals(0, alertCollector.getCollected().size());
        assertEquals(1, passCollector.getCollected().size());

        processAndCollect(node, highTemp);
        assertEquals(0, alertCollector.getCollected().size());
        assertEquals(2, passCollector.getCollected().size());
    }
}