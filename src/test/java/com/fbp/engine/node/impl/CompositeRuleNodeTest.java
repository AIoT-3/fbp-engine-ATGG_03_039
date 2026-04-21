package com.fbp.engine.node.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fbp.engine.core.Connection;
import com.fbp.engine.message.Message;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("CompositeRuleNode 테스트")
class CompositeRuleNodeTest {

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

    private void processAndCollect(CompositeRuleNode node, Message msg) {
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
    @DisplayName("1. AND - 모두 만족")
    void testAndAllMatch() {
        CompositeRuleNode node = new CompositeRuleNode("and-rule", CompositeRuleNode.Operator.AND);
        node.addCondition("temperature", ">", 30.0);
        node.addCondition("humidity", ">", 70.0);

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("temperature", 35.0, "humidity", 80.0));
        processAndCollect(node, msg);

        assertEquals(1, matchCollector.getCollected().size());
        assertEquals(0, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("2. AND - 하나 불만족")
    void testAndOneMismatch() {
        CompositeRuleNode node = new CompositeRuleNode("and-rule", CompositeRuleNode.Operator.AND);
        node.addCondition("temperature", ">", 30.0);
        node.addCondition("humidity", ">", 70.0);

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("temperature", 35.0, "humidity", 50.0));
        processAndCollect(node, msg);

        assertEquals(0, matchCollector.getCollected().size());
        assertEquals(1, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("3. OR - 하나 만족")
    void testOrOneMatch() {
        CompositeRuleNode node = new CompositeRuleNode("or-rule", CompositeRuleNode.Operator.OR);
        node.addCondition("temperature", ">", 30.0);
        node.addCondition("humidity", ">", 70.0);

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("temperature", 25.0, "humidity", 80.0));
        processAndCollect(node, msg);

        assertEquals(1, matchCollector.getCollected().size());
        assertEquals(0, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("4. OR - 모두 불만족")
    void testOrAllMismatch() {
        CompositeRuleNode node = new CompositeRuleNode("or-rule", CompositeRuleNode.Operator.OR);
        node.addCondition("temperature", ">", 30.0);
        node.addCondition("humidity", ">", 70.0);

        node.getOutputPort("match").connect(matchConnection);
        node.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg = new Message(Map.of("temperature", 25.0, "humidity", 50.0));
        processAndCollect(node, msg);

        assertEquals(0, matchCollector.getCollected().size());
        assertEquals(1, mismatchCollector.getCollected().size());
    }

    @Test
    @DisplayName("5. 빈 조건")
    void testEmptyConditions() {
        CompositeRuleNode andNode = new CompositeRuleNode("and-empty", CompositeRuleNode.Operator.AND);
        andNode.getOutputPort("match").connect(matchConnection);
        andNode.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg1 = new Message(Map.of("temperature", 35.0));
        processAndCollect(andNode, msg1);

        assertEquals(0, matchCollector.getCollected().size());
        assertEquals(1, mismatchCollector.getCollected().size());

        matchCollector.getCollected().clear();
        mismatchCollector.getCollected().clear();

        CompositeRuleNode orNode = new CompositeRuleNode("or-empty", CompositeRuleNode.Operator.OR);
        orNode.getOutputPort("match").connect(matchConnection);
        orNode.getOutputPort("mismatch").connect(mismatchConnection);

        Message msg2 = new Message(Map.of("temperature", 35.0));
        processAndCollect(orNode, msg2);

        assertEquals(0, matchCollector.getCollected().size());
        assertEquals(1, mismatchCollector.getCollected().size());
    }
}