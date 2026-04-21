package com.fbp.engine.node.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fbp.engine.core.Connection;
import com.fbp.engine.message.Message;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("RuleNode 테스트")
class RuleNodeTest {

    private CollectorNode matchCollector;
    private CollectorNode mismatchCollector;
    private Connection matchConnection;
    private Connection mismatchConnection;

    @BeforeEach
    void setUp() {
        matchCollector = new CollectorNode("match-collector");
        mismatchCollector = new CollectorNode("mismatch-collector");

        matchConnection = new Connection("toMatch");
        matchConnection.setTarget(matchCollector.getInputPort("in"));

        mismatchConnection = new Connection("toMismatch");
        mismatchConnection.setTarget(mismatchCollector.getInputPort("in"));
    }

    private void processAndCollect(RuleNode node, Message msg) {
        node.process(msg);

        if (matchConnection.getBufferSize() > 0) {
            Message matchMsg = matchConnection.poll();
            if (matchMsg != null) {
                matchCollector.process(matchMsg);
            }
        }

        if (mismatchConnection.getBufferSize() > 0) {
            Message mismatchMsg = mismatchConnection.poll();
            if (mismatchMsg != null) {
                mismatchCollector.process(mismatchMsg);
            }
        }
    }

    @Test
    @DisplayName("1. 조건 만족 → match")
    void testConditionMet() {
        RuleNode node = new RuleNode("rule-node", msg -> {
            Double temp = (Double) msg.getPayload().get("temperature");
            return temp != null && temp > 30.0;
        });

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("temperature", 35.0));
        processAndCollect(node, msg);

        assertEquals(1, matchCollector.getCollected().size());
        assertEquals(0, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("2. 조건 불만족 → mismatch")
    void testConditionNotMet() {
        RuleNode node = new RuleNode("rule-node", msg -> {
            Double temp = (Double) msg.getPayload().get("temperature");
            return temp != null && temp > 30.0;
        });

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("temperature", 25.0));
        processAndCollect(node, msg);

        assertEquals(0, matchCollector.getCollected().size());
        assertEquals(1, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("3. 포트 구성")
    void testPorts() {
        RuleNode node = new RuleNode("rule-node", msg -> true);

        assertNotNull(node.getInputPort("in"));
        assertNotNull(node.getOutputPort("match"));
        assertNotNull(node.getOutputPort("mismatch"));
    }

    @Test
    @DisplayName("4. null 필드 처리")
    void testNullFieldHandling() {
        RuleNode node = new RuleNode("rule-node", msg -> {
            Double temp = (Double) msg.getPayload().get("temperature");
            return temp != null && temp > 30.0;
        });

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("humidity", 50.0));

        assertDoesNotThrow(() -> processAndCollect(node, msg));

        assertEquals(0, matchCollector.getCollected().size());
        assertEquals(1, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("5. 다수 메시지 분기")
    void testMultipleMessagesRouting() {
        RuleNode node = new RuleNode("rule-node", msg -> {
            Double temp = (Double) msg.getPayload().get("temperature");
            return temp != null && temp > 30.0;
        });

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        processAndCollect(node, new Message(Map.of("temperature", 35.0)));
        processAndCollect(node, new Message(Map.of("temperature", 25.0)));
        processAndCollect(node, new Message(Map.of("temperature", 40.0)));
        processAndCollect(node, new Message(Map.of("temperature", 10.0)));

        assertEquals(2, matchCollector.getCollected().size());
        assertEquals(2, mismatchCollector.getCollected().size());
    }
}