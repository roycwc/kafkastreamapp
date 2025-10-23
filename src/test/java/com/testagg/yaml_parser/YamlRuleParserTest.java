package com.testagg.yaml_parser;

import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.testagg.Parsers.Condition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class YamlRuleParserTest {

    @Test
    @DisplayName("Able to parse a rule with multiple conditions")
    public void shouldParseYamlRuleWithMultipleConditions() {
        String yamlRuleStr = """
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

        var yamlParser = new Yaml();
        Map<Object, Object> yamlRuleObj = yamlParser.load(yamlRuleStr);
        var mapper = new ObjectMapper();
        mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        var yamlRule = mapper.convertValue(yamlRuleObj, Condition.class);
        assertEquals(2, yamlRule.expressions.size());

        var condition1 = yamlRule.expressions.get(0);
        assertEquals(2, condition1.expressions.size());
        var expr1 = condition1.expressions.get(0);
        assertEquals("transaction_amount", expr1.field);
        assertEquals("GT", expr1.expressionOperator.name());
        assertEquals(5000, expr1.value);
        var expr2 = condition1.expressions.get(1);
        assertEquals("country", expr2.field);
        assertEquals("EQ", expr2.expressionOperator.name());
        assertEquals("US", expr2.value);
    }
}
