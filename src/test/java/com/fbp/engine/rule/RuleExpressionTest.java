package com.fbp.engine.rule;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fbp.engine.message.Message;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("RuleExpression 테스트")
class RuleExpressionTest {

    @Test
    @DisplayName("1. 파싱 - 숫자 비교")
    void testNumberEvaluation() {
        RuleExpression exp = RuleExpression.parse("temperature > 30.0");

        Message validMessage = new Message(Map.of("temperature", 35.5));
        Message invalidMessage = new Message(Map.of("temperature", 25.0));

        assertTrue(exp.evaluate(validMessage));
        assertFalse(exp.evaluate(invalidMessage));
    }

    @Test
    @DisplayName("2. 파싱 - 문자열 비교")
    void testStringEvaluation() {
        RuleExpression exp1 = RuleExpression.parse("status == 'ON'");
        RuleExpression exp2 = RuleExpression.parse("status == ON");

        Message validMessage = new Message(Map.of("status", "ON"));
        Message invalidMessage = new Message(Map.of("status", "OFF"));

        assertTrue(exp1.evaluate(validMessage));
        assertFalse(exp1.evaluate(invalidMessage));

        assertTrue(exp2.evaluate(validMessage));
        assertFalse(exp2.evaluate(invalidMessage));
    }

    @Test
    @DisplayName("3. 모든 연산자 검증")
    void testAllOperators() {
        Message msg = new Message(Map.of("value", 50.0));

        assertTrue(RuleExpression.parse("value > 40").evaluate(msg));
        assertFalse(RuleExpression.parse("value > 50").evaluate(msg));

        assertTrue(RuleExpression.parse("value >= 50").evaluate(msg));
        assertFalse(RuleExpression.parse("value >= 60").evaluate(msg));

        assertTrue(RuleExpression.parse("value < 60").evaluate(msg));
        assertFalse(RuleExpression.parse("value < 50").evaluate(msg));

        assertTrue(RuleExpression.parse("value <= 50").evaluate(msg));
        assertFalse(RuleExpression.parse("value <= 40").evaluate(msg));

        assertTrue(RuleExpression.parse("value == 50").evaluate(msg));
        assertFalse(RuleExpression.parse("value == 40").evaluate(msg));

        assertTrue(RuleExpression.parse("value != 40").evaluate(msg));
        assertFalse(RuleExpression.parse("value != 50").evaluate(msg));
    }

    @Test
    @DisplayName("4. 잘못된 표현식")
    void testInvalidExpression() {
        assertThrows(IllegalArgumentException.class, () -> {
            RuleExpression.parse("temperature 30.0");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            RuleExpression.parse("temperature >> 30.0");
        });
    }

    @Test
    @DisplayName("5. 필드 없음")
    void testMissingField() {
        RuleExpression exp = RuleExpression.parse("temperature > 30.0");

        Message msgWithoutField = new Message(Map.of("humidity", 50.0));
        Message emptyMsg = new Message(Map.of());

        assertFalse(exp.evaluate(msgWithoutField));
        assertFalse(exp.evaluate(emptyMsg));
    }
}