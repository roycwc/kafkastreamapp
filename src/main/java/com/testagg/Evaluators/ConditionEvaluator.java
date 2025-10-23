package com.testagg.Evaluators;

import java.util.Map;

import com.testagg.Parsers.Condition;
import com.testagg.Parsers.ConditionOperator;

public class ConditionEvaluator {
    private Condition condition;

    public ConditionEvaluator(Condition newCondition) {
        this.condition = newCondition;
    }

    public boolean evaluate(Map<String, Object> jsonObj) {
        if (condition.expressions == null || condition.expressions.isEmpty()) {
            var expressionEvaluator = new ExpressionEvaluator(condition);
            return expressionEvaluator.evaluate(jsonObj);
        }
        if (condition.conditionOperator == null) {
            return false;
        }
        if (condition.conditionOperator.equals(ConditionOperator.ALL)) {
            return condition.expressions.stream()
                    .allMatch(innerCondition -> {
                        var innerEvaluator = new ConditionEvaluator(innerCondition);
                        return innerEvaluator.evaluate(jsonObj);
                    });
        }
        if (condition.conditionOperator.equals(ConditionOperator.ANY)) {
            return condition.expressions.stream()
                    .anyMatch(innerCondition -> {
                        var innerEvaluator = new ConditionEvaluator(innerCondition);
                        return innerEvaluator.evaluate(jsonObj);
                    });
        }

        if (condition.conditionOperator.equals(ConditionOperator.NONE)) {
            return condition.expressions.stream()
                    .noneMatch(innerCondition -> {
                        var innerEvaluator = new ConditionEvaluator(innerCondition);
                        return innerEvaluator.evaluate(jsonObj);
                    });
        }
        return false;
    }
}
