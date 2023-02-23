package ru.gavr.bdd4hadoop.dsl.operations

import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.assertions.Assertions
import ru.gavr.bdd4hadoop.assertions.AssertionsTable

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class Expect implements Serializable {

    Map<String, Closure> exceptionAssertions = [:]
    Map<String, Closure> resultAssertions = [:]

    def exception() {
        [
                text: [
                        contains    : { expected -> exceptionAssertions["exception_contains_$expected"] =  { String received ->
                            Assertions.isContains("exception", expected, received) }
                        },
                        notcontains : { expected -> exceptionAssertions["exception_not_contains_$expected"] =  { String received ->
                            Assertions.isNotContains("exception", expected, received) }
                        }
                ]
        ]
    }

    def result() {
        [
                text: [
                        contains    : { expected -> resultAssertions["result_contains_$expected"] =  { def received ->
                            Assertions.isContains("result", expected, JsonOutput.toJson(received)) }
                        },
                        notcontains : { expected -> resultAssertions["result_not_contains_$expected"] =  { def received ->
                            Assertions.isNotContains("result", expected, JsonOutput.toJson(received)) }
                        },
                        equalsTo    : { expected -> resultAssertions["result_equals_$expected"] =  { def received ->
                            Assertions.equalsTo("result", expected, JsonOutput.toJson(received)) }
                        },
                ],
                is_Empty   : { -> resultAssertions["result_is_empty"] = { def received -> Assertions.isEmpty(JsonOutput.toJson(received)) } },
                isNothing  : { -> resultAssertions["result_is_nothing"] = { def received -> Assertions.isNothing(JsonOutput.toJson(received)) } },
                file: [
                        containsLine : { Integer lineNum, String line ->
                            resultAssertions["lineNum_${lineNum}_containsLine_${line}"] = {
                                List<String> fileLines -> Assertions.containsLine(fileLines, lineNum, line) }
                        },
                        lineStartWith : { Integer lineNum, String line ->
                            resultAssertions["lineNum_${lineNum}_lineStartWith_${line}"] = {
                                List<String> fileLines -> Assertions.lineStartWith(fileLines, lineNum, line) }
                        }
                ],
                table: [
                        cell: [
                                equalsStr: { Integer rowNum, String fieldName, String expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsStr_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEquals(received, rowNum, fieldName, expectedValue) }
                                },
                                notEqualsStr: { Integer rowNum, String fieldName, String expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsStr_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellNotEquals(received, rowNum, fieldName, expectedValue) }
                                },
                                isEmptyStr: { Integer rowNum, String fieldName ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_isEmpty"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.isCellEmptyStr(received, rowNum, fieldName) }
                                },
                                equalsInt: { Integer rowNum, String fieldName, Integer expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsInt_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEquals(received, rowNum, fieldName, expectedValue) }
                                },
                                equalsLong: { Integer rowNum, String fieldName, Long expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsLong_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEquals(received, rowNum, fieldName, expectedValue) }
                                },
                                equalsBoolean: { Integer rowNum, String fieldName, Boolean expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsBoolean_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEquals(received, rowNum, fieldName, expectedValue) }
                                },
                                equalsDouble: { Integer rowNum, String fieldName, Double expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsDouble_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEquals(received, rowNum, fieldName, expectedValue) }
                                },
                                equalsDate: { Integer rowNum, String fieldName, String expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsDate_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEqualsDate(received, rowNum, fieldName, expectedValue) }
                                },
                                isNull: { Integer rowNum, String fieldName ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_is_null"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellIsNull(received, rowNum, fieldName)
                                    }
                                },
                                startsWith: { Integer rowNum, String fieldName, String expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_startsWith_$expectedValue"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellStartsWith(received, rowNum, fieldName, expectedValue) }
                                },
                                equalsStrDecodeBase64: { Integer rowNum, String fieldName, String expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_equalsStrDecodeBase64_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellEqualsDecodeBase64(received, rowNum, fieldName, expectedValue) }
                                },
                                startsWithDecodeBase64: { Integer rowNum, String fieldName, String expectedValue ->
                                    resultAssertions["rowNum_${rowNum}_fieldName_${fieldName}_startsWithDecodeBase64_$expectedValue"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.cellStartsWithDecodeBase64(received, rowNum, fieldName, expectedValue) }
                                },
                        ],
                        column: [
                                equalsStr: { String columnName, String expectedValue ->
                                    resultAssertions["column_${columnName}_equalsStr_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnEquals(received, columnName, expectedValue) }
                                },
                                isEmptyStr: { String columnName ->
                                    resultAssertions["column_${columnName}_isEmpty"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.isColumnEmptyStr(received, columnName) }
                                },
                                equalsInt: { String columnName, Integer expectedValue ->
                                    resultAssertions["column_${columnName}_equalsInt_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnEquals(received, columnName, expectedValue) }
                                },
                                equalsLong: { String columnName, Long expectedValue ->
                                    resultAssertions["column_${columnName}_equalsLong_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnEquals(received, columnName, expectedValue) }
                                },
                                equalsBoolean: { String columnName, Boolean expectedValue ->
                                    resultAssertions["column_${columnName}_equalsBoolean_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnEquals(received, columnName, expectedValue) }
                                },
                                equalsDouble: { String columnName, Double expectedValue ->
                                    resultAssertions["column_${columnName}_equalsDouble_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnEquals(received, columnName, expectedValue) }
                                },
                                equalsDate: { String columnName, String expectedValue ->
                                    resultAssertions["column_${columnName}_equalsDate_${expectedValue}"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnEqualsDate(received, columnName, expectedValue) }
                                },
                                isNull: { String columnName ->
                                    resultAssertions["column_${columnName}_is_null"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnIsNull(received, columnName)
                                    }
                                },
                                startsWith: { String columnName, String expectedValue ->
                                    resultAssertions["column_${columnName}_startsWith_$expectedValue"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnStartsWith(received, columnName, expectedValue) }
                                },
                                contains: { String columnName, def expectedValue ->
                                    resultAssertions["column_${columnName}_contains_$expectedValue"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnContains(received, columnName, expectedValue) }
                                },
                                notContains: { String columnName, def expectedValue ->
                                    resultAssertions["column_${columnName}_contains_$expectedValue"] = {
                                        List<Map<String, Object>> received -> AssertionsTable.columnNotContains(received, columnName, expectedValue) }
                                }
                        ],
                        notContainsField: { String fieldName ->
                            resultAssertions["not_contains_${fieldName}"] = {
                                List<Map<String, Object>> received -> AssertionsTable.isNotContainsField(received, fieldName) }
                        },
                        containsField: { String fieldName ->
                            resultAssertions["contains_${fieldName}"] = {
                                List<Map<String, Object>> received -> AssertionsTable.isContainsField(received, fieldName) }
                        },
                        countRows: { Integer expected ->
                            resultAssertions["countCell_expected_$expected"] = {
                                List<Map<String, Object>> received -> AssertionsTable.countRows(received, expected)
                            }
                        },
                        tableEquals: { String path ->
                            resultAssertions["tableEquals_expected_${path}"] = {
                                List<Map<String, Object>> received -> AssertionsTable.tableEquals(received, path)
                            }
                        }
                ],
        ]
    }
}
