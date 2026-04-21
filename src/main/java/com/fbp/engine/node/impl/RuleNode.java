package com.fbp.engine.node.impl;

import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;
import com.fbp.engine.rule.RuleExpression;
import java.util.function.Predicate;

/**
 * input port : in
 * output port : match, mismatch (조건이 맞을 땐 match / 맞지않을 때 mismatch)
 *
 * 조건식을 평가하여 메시지의 경로를 결정함
 *                 -> match
 * 메시지 -> 조건식 |
 *                 -> mismatch
 */
public class RuleNode extends AbstractNode {
    private Predicate<Message> condition;

    /**
     * 사용법
     * 장점: 구현 간단, 타입 안전, IDE 지원
     * 단점: 규칙 변경 시 코드 수정 및 재컴파일 필요
     *
     * RuleNode rule = new RuleNode("temp-rule", msg -> {
     *     Double temp = msg.get("temperature");
     *     return temp != null && temp > 30.0;
     * });
     */
    public RuleNode(String id, Predicate<Message> condition) {
        super(id);
        this.condition = condition;
        addInputPort("in");
        addOutputPort("match");
        addOutputPort("mismatch");
    }

    /**
     * 사용법
     * 장점: 설정 파일에서 규칙 정의 가능, 런타임 변경 가능
     * 단점: 조건식 파서 구현 필요, 타입 안정성 낮음
     *
     * RuleNode rule = new RuleNode("temp-rule", "temperature > 30.0");
     */
    public RuleNode(String id, String expression) {
        super(id);
        RuleExpression ruleExpression = RuleExpression.parse(expression);
        this.condition = ruleExpression::evaluate;
        addInputPort("in");
        addOutputPort("match");
        addOutputPort("mismatch");
    }

    @Override
    protected void onProcess(Message message) {
        if(condition.test(message)){
            send("match", message);
        }else{
            send("mismatch", message);
        }
    }
}
