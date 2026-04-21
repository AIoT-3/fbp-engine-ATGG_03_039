package com.fbp.engine.rule;

import com.fbp.engine.message.Message;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RuleExpression {
    private String field;
    private String operator;
    private Object value;

    private static final Pattern PATTERN = Pattern.compile("^\\s*([a-zA-Z0-9_]+)\\s*(==|!=|>=|<=|>|<)\\s*(?![=><])(.+)\\s*$");

    public RuleExpression(String field, String operator, Object value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public static RuleExpression parse(String expression){
        Matcher matcher = PATTERN.matcher(expression);
        if(!matcher.matches()){
            throw new IllegalArgumentException("지원하지 않는 조건식 형식입니다: "+ expression);
        }
        String field = matcher.group(1);
        String operator = matcher.group(2);
        String valueStr = matcher.group(3).trim();

        Object targetValue;
        if ((valueStr.startsWith("'") && valueStr.endsWith("'")) ||
                (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
            targetValue = valueStr.substring(1, valueStr.length() - 1);
        } else {
            try {
                targetValue = Double.parseDouble(valueStr);
            } catch (NumberFormatException e) {
                if (valueStr.equalsIgnoreCase("true") || valueStr.equalsIgnoreCase("false")) {
                    targetValue = Boolean.parseBoolean(valueStr);
                } else {
                    targetValue = valueStr;
                }
            }
        }
        return new RuleExpression(field,operator,targetValue);

    }

    public boolean evaluate(Message message){
        Object actualValue = message.getPayload().get(field);
        if(actualValue==null){
            return false;
        }

        if(value instanceof Double){
            if(!(actualValue instanceof Number)) return false;
            double actualDouble = ((Number) actualValue).doubleValue();
            double targetDouble = (Double) value;

            switch (operator){
                case ">" : return actualDouble > targetDouble;
                case ">=" : return actualDouble>=targetDouble;
                case "<" : return actualDouble<targetDouble;
                case "<=" : return actualDouble<=targetDouble;
                case "==" : return actualDouble==targetDouble;
                case "!=" : return actualDouble!=targetDouble;
                default: return false;
            }
        }else if (value instanceof String){
            String actualStr = actualValue.toString();
            String targetStr = (String)value;

            switch (operator){
                case "==" : return actualStr.equals(targetStr);
                case "!=" : return !actualStr.equals(targetStr);
                default: throw  new IllegalArgumentException("문자열은 ==, !=만 적용 가능");
            }

        }else if (value instanceof Boolean) {
            if (!(actualValue instanceof Boolean)) return false;
            boolean actualBool = (Boolean) actualValue;
            boolean targetBool = (Boolean) value;

            switch (operator) {
                case "==": return actualBool == targetBool;
                case "!=": return actualBool != targetBool;
                default: throw new IllegalArgumentException("Boolean에는 ==, != 연산자만 사용할 수 있습니다.");
            }
        }
        return false;
    }
}
