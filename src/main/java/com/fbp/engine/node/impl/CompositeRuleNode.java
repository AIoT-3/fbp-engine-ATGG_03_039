package com.fbp.engine.node.impl;

import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;
import com.fbp.engine.rule.RuleExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 *  input port : in
 *  output port : match, mismatch (조건이 맞을 땐 match / 맞지않을 때 mismatch)
 *
 *  CompositeRule rule = new CompositeRule("complex", CompositeRule.Operator.AND);
 *  rule.addCondition("temperature", ">", 30.0);
 *  rule.addCondition("humidity", ">", 70.0);
 */
public class CompositeRuleNode extends AbstractNode {
    public enum Operator{
        AND,
        OR
    }
    private final List<Predicate<Message>> conditions = new ArrayList<>();
    private final Operator operator;

    public CompositeRuleNode(String id, Operator operator) {
        super(id);
        this.operator = operator;
        addInputPort("in");
        addOutputPort("match");
        addOutputPort("mismatch");
    }

    public CompositeRuleNode addCondition(Predicate<Message> condition){
        this.conditions.add(condition);
        return this;
    }
    public CompositeRuleNode addCondition(String field, String op, Object value){
        String valueStr = (value instanceof String) ? "'" + value + "'" : value.toString();
        String expression = field + " " + op + " "+ valueStr;

        RuleExpression ruleExpression = RuleExpression.parse(expression);
        this.conditions.add(ruleExpression::evaluate);
        return this;
    }


    @Override
    protected void onProcess(Message message) {
        if(conditions.isEmpty()){
            send("mismatch", message);
            return;
        }

        boolean isMatch = (operator ==Operator.AND);

        for(Predicate<Message> condition : conditions){
            boolean result = condition.test(message);

            if(operator == Operator.AND){
                isMatch = isMatch && result;
                if(!isMatch) break;
            }else if (operator == Operator.OR){
                isMatch = isMatch || result;
                if(isMatch) break;
            }
        }

        if(isMatch){
            send("match", message);
        }else{
            send("mismatch", message);
        }
    }
}
