package ru.gavr.bdd4hadoop.assertions

import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.apache.commons.collections.CollectionUtils
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.connectors.HDFSParquetReader

import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class AssertionsTable {

    static def cellEquals(List<Map<String, Object>> received, Integer rowNum, String fieldName, def expectedValue){
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isEquals(received, rowNum, fieldName, expectedValue)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def cellNotEquals(List<Map<String, Object>> received, Integer rowNum, String fieldName, def expectedValue){
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isNotEquals(received, rowNum, fieldName, expectedValue)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def isCellEmptyStr(List<Map<String, Object>> received, Integer rowNum, String fieldName){
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isEmptyStr(received, rowNum, fieldName)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def cellEqualsDecodeBase64(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValue){
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isEqualsBase64(received, rowNum, fieldName, expectedValue)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def cellEqualsDate(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValueStr){
        ZonedDateTime expectedValue = ZonedDateTime.of(LocalDateTime.parse(expectedValueStr), ZoneId.of("Europe/Moscow"))
        return cellEquals(received, rowNum, fieldName, expectedValue)
    }

    static def cellIsNull(List<Map<String, Object>> received, Integer rowNum, String fieldName){
        return cellEquals(received, rowNum, fieldName, null)
    }

    static def cellStartsWith(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValue) {
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isStartsWith(received, rowNum, fieldName, expectedValue)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def cellStartsWithDecodeBase64(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValue) {
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isStartsWithBase64(received, rowNum, fieldName, expectedValue)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def columnEquals(List<Map<String, Object>> received, String columnName, def expectedValue){
        def assertionResult = false
        def expectedMsg, receivedMsg

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else {
            for (int i = 0; i < received.size(); i++) {
                (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isEquals(received, i, columnName, expectedValue)
                if (!assertionResult)
                    break
            }
        }
        if (expectedValue.getClass().getSimpleName() == "ZonedDateTime") {
            expectedValue = AssertionsTableUtils.FORMAT.format(expectedValue)
        }
        expectedMsg = "received data column \"$columnName\" equals \"$expectedValue\""
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def isColumnEmptyStr(List<Map<String, Object>> received, String columnName){
        def assertionResult = false
        def expectedMsg, receivedMsg

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else {
            for (int i = 0; i < received.size(); i++) {
                (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isEmptyStr(received, i, columnName)
                if (!assertionResult)
                    break
            }
        }
        expectedMsg = "received data column \"$columnName\" is Empty"
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def columnEqualsDate(List<Map<String, Object>> received, String columnName, String expectedValueStr) {
        ZonedDateTime expectedValue = ZonedDateTime.of(LocalDateTime.parse(expectedValueStr), ZoneId.of("Europe/Moscow"))
        return columnEquals(received, columnName, expectedValue)
    }

    static def columnIsNull(List<Map<String, Object>> received, String columnName){
        return columnEquals(received, columnName, null)
    }

    static def columnStartsWith(List<Map<String, Object>> received, String columnName, String expectedValue){
        def assertionResult = false
        def expectedMsg, receivedMsg

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else {
            for (int i = 0; i < received.size(); i++) {
                (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isStartsWith(received, i, columnName, expectedValue)
                if (!assertionResult)
                    break
            }
        }
        expectedMsg = "parquet record column \"$columnName\" starts with \"$expectedValue\""
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def columnContains(List<Map<String, Object>> received, String columnName, def expectedValue) {
        def assertionResult = false
        def expectedMsg, receivedMsg
        Set<Object> receivedSet = new HashSet<>()

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else {
            def res = received.stream()
                    .filter({
                        def value = it.get(columnName)
                        receivedSet.add(value)
                        return value == expectedValue
                    })
                    .find()
            assertionResult = res != null
            if (!assertionResult) {
                receivedMsg = "received data column \"$columnName\" doesn't contain \"$expectedValue\". Contains: $receivedSet"
            }
        }
        expectedMsg = "received data column \"$columnName\" contains \"$expectedValue\""
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def columnNotContains(List<Map<String, Object>> received, String columnName, def expectedValue) {
        def assertionResult = false
        def expectedMsg, receivedMsg
        Set<Object> receivedSet = new HashSet<>()

        if (received == null) {
            receivedMsg = "there aren't received data"
        } else {
            def res = received.stream()
                    .filter({
                        def value = it.get(columnName)
                        receivedSet.add(value)
                        return value == expectedValue
                    })
                    .find()
            assertionResult = res == null
            if (!assertionResult) {
                receivedMsg = "received data column \"$columnName\" contain \"$expectedValue\". Contains: $receivedSet"
            }
        }
        expectedMsg = "received data column \"$columnName\" doesn't contain \"$expectedValue\""
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def isContainsField(List<Map<String, Object>> received, String fieldName){
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isContainsFieldUtils(received, fieldName)
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def isNotContainsField(List<Map<String, Object>> received, String fieldName){
        def (assertionResult, expectedMsg, receivedMsg) = AssertionsTableUtils.isContainsFieldUtils(received, fieldName)
        if (receivedMsg.equals("received data is empty")) {
            return Assertions.checkResult(false, "received data does not contain field \"$fieldName\"", "received data is empty")
        } else {
            return Assertions.checkResult(!assertionResult, "received data does not contain field \"$fieldName\"", "received data contains field \"$fieldName\"")
        }
    }

    static def countRows(List<Map<String, Object>> received, Integer expectedCountRow){
        def assertionResult = false
        def expectedMsg = "received data has ${expectedCountRow} rows"
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (received.size() != expectedCountRow) {
            receivedMsg = "received data has ${received.size()} rows"
        } else {
            assertionResult = true
        }
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def tableEquals(List<Map<String, Object>> received, List<Map<String, Object>> expected) {
        //не работает если тип значения поля массив
        def assertionResult = false
        def expectedMsg = "received data equals expected"
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (expected == null || expected.isEmpty()) {
            receivedMsg = "expected data is empty"
        } else {
            assertionResult = CollectionUtils.isEqualCollection(received, expected)
        }

        if (!assertionResult) {
            receivedMsg = "received data doesn't equals expected"
        }
        return Assertions.checkResult(assertionResult, expectedMsg, receivedMsg)
    }

    static def tableEquals(List<Map<String, Object>> received, String path) {
        List<Map<String, Object>> expected = HDFSParquetReader.readData(path)
        return tableEquals(received, expected)
    }
}
