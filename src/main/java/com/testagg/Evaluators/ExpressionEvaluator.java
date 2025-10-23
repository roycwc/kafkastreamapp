package com.testagg.Evaluators;

import java.util.Map;

import com.testagg.Parsers.Expression;
import com.testagg.Parsers.ExpressionOperator;

public class ExpressionEvaluator {

    private Expression expression;

    public ExpressionEvaluator(Expression newExpression) {
        expression = newExpression;
    }

    public boolean evaluate(Map<String, Object> jsonObj) {
        // Implement the evaluation logic here
        if (expression.expressionOperator == null) {
            return false;
        }
        Object fieldValue = jsonObj.get(expression.field);
        if (fieldValue == null) {
            return false;
        }
        if (expression.value != null) {
            if (expression.value instanceof String) {
                if (expression.expressionOperator.equals(ExpressionOperator.EQ)){
                    return fieldValue.toString().equals((String)expression.value);
                }
                if (expression.expressionOperator.equals(ExpressionOperator.NOT)){
                    return !fieldValue.toString().equals((String)expression.value);
                }
                if (expression.expressionOperator.equals(ExpressionOperator.CONTAINS)){
                    return fieldValue.toString().contains((String)expression.value);
                }
                return false;
            }
            if (expression.value instanceof Integer) {
                if (expression.expressionOperator.equals(ExpressionOperator.EQ)){
                    return Integer.parseInt(fieldValue.toString()) == (Integer)expression.value;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.GT)){
                    return Integer.parseInt(fieldValue.toString()) > (Integer)expression.value;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.LT)){
                    return Integer.parseInt(fieldValue.toString()) < (Integer)expression.value;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.GTE)){
                    return Integer.parseInt(fieldValue.toString()) >= (Integer)expression.value;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.LTE)){
                    return Integer.parseInt(fieldValue.toString()) <= (Integer)expression.value;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.NOT)){
                    return Integer.parseInt(fieldValue.toString()) != (Integer)expression.value;
                }
                return false;
            }
            if (expression.value instanceof Boolean) {
                if (expression.expressionOperator.equals(ExpressionOperator.EQ)){
                    return Boolean.parseBoolean(fieldValue.toString()) == (Boolean)expression.value;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.NOT)){
                    return Boolean.parseBoolean(fieldValue.toString()) != (Boolean)expression.value;
                }
                return false;
            }
            return false;
        }
        if (expression.values != null && !expression.values.isEmpty()) {
            if (fieldValue instanceof String) {
                if (expression.expressionOperator.equals(ExpressionOperator.IN)){
                    return expression.values.contains(fieldValue.toString());
                }
                if (expression.expressionOperator.equals(ExpressionOperator.NOT_IN)){
                    return !expression.values.contains(fieldValue.toString());
                }
                if (expression.expressionOperator.equals(ExpressionOperator.ANY)){
                    for (Object val : expression.values) {
                        if (fieldValue.toString().contains((String)val)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.ALL)){
                    for (Object val : expression.values) {
                        if (!fieldValue.toString().contains((String)val)) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            if (fieldValue instanceof Integer) {
                if (expression.expressionOperator.equals(ExpressionOperator.IN)){
                    for (Object val : expression.values) {
                        if (Integer.parseInt(fieldValue.toString()) == (Integer)val) {
                            return true;
                        }
                    }
                    return false;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.NOT_IN)){
                    for (Object val : expression.values) {
                        if (Integer.parseInt(fieldValue.toString()) == (Integer)val) {
                            return false;
                        }
                    }
                    return true;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.ANY)){
                    for (Object val : expression.values) {
                        if (Integer.parseInt(fieldValue.toString()) == (Integer)val) {
                            return true;
                        }
                    }
                    return false;
                }
                if (expression.expressionOperator.equals(ExpressionOperator.ALL)){
                    for (Object val : expression.values) {
                        if (Integer.parseInt(fieldValue.toString()) != (Integer)val) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }
        return false;
    }

}
