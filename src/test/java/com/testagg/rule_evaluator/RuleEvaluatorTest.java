package com.testagg.rule_evaluator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.testagg.Evaluators.ConditionEvaluator;
import com.testagg.Parsers.Condition;

public class RuleEvaluatorTest {

    @Test
    @DisplayName("Able to parse a yaml rule and evaluate it against json input data")
    public void shouldParseYamlRuleAndEvaluateJsonInput() throws JsonMappingException, JsonProcessingException {
        var sampleExpenseJsonStr = """
                {
                    "transaction_amount": 6000,
                    "country": "US",
                    "account_age_days": 20,
                    "is_high_risk_country": false
                }
                """;

        var yamlRuleStr = """
                 conditionOperator: ALL
                 expressions:
                    - conditionOperator: ALL
                      expressions:
                        - field: transaction_amount
                          expressionOperator: GT
                          value: 5000
                        - field: country
                          expressionOperator: EQ
                          value: US
                    - conditionOperator: ANY
                      expressions:
                        - field: account_age_days
                          expressionOperator: LT
                          value: 30
                        - field: is_high_risk_country
                          expressionOperator: EQ
                          value: true
                """;

        var mapper1 = new ObjectMapper();
        var sampleExpense = mapper1.readValue(sampleExpenseJsonStr, new TypeReference<Map<String, Object>>() {
        });

        var yamlParser = new Yaml();
        Map<Object, Object> yamlRuleObj = yamlParser.load(yamlRuleStr);
        var mapper2 = new ObjectMapper();
        mapper2.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        var yamlRule = mapper2.convertValue(yamlRuleObj, Condition.class);

        var conditionEvaluator = new ConditionEvaluator(yamlRule);
        var result = conditionEvaluator.evaluate(sampleExpense);
        assertTrue(result);
    }

    @Test
    @DisplayName("Able to parse a yaml rule and evaluate it against json input data with negative result")
    public void shouldParseYamlRuleAndEvaluateJsonInputWithNegativeResult()
            throws JsonMappingException, JsonProcessingException {
        var sampleExpenseJsonStr = """
                {
                    "transaction_amount": 4000,
                    "country": "HK",
                    "account_age_days": 20,
                    "is_high_risk_country": true
                }
                """;

        var yamlRuleStr = """
                 conditionOperator: ALL
                 expressions:
                    - conditionOperator: ALL
                      expressions:
                        - field: transaction_amount
                          expressionOperator: GT
                          value: 5000
                        - field: country
                          expressionOperator: EQ
                          value: US
                    - conditionOperator: ANY
                      expressions:
                        - field: account_age_days
                          expressionOperator: LT
                          value: 30
                        - field: is_high_risk_country
                          expressionOperator: EQ
                          value: true
                """;

        var mapper1 = new ObjectMapper();
        var sampleExpense = mapper1.readValue(sampleExpenseJsonStr, new TypeReference<Map<String, Object>>() {
        });

        var yamlParser = new Yaml();
        Map<Object, Object> yamlRuleObj = yamlParser.load(yamlRuleStr);
        var mapper2 = new ObjectMapper();
        mapper2.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        var yamlRule = mapper2.convertValue(yamlRuleObj, Condition.class);

        var conditionEvaluator = new ConditionEvaluator(yamlRule);
        var result = conditionEvaluator.evaluate(sampleExpense);
        assertFalse(result);
    }

}
