package ru.gavr.bdd4hadoop.utils

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.apache.parquet.tools.read.SimpleRecord
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import static org.apache.commons.lang.time.DateUtils.MILLIS_IN_DAY

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class AssertionsTableUtils {
    static def FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS")

    static def getDateStrFromBytes(def val, def expectedValue) {
        final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1)
        final long JULIAN_EPOCH_OFFSET_DAYS = 2_440_588

        byte[] bytes = val as byte[]
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0])
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8])
        long nanoseconds = ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY) + (timeOfDayNanos / NANOS_PER_MILLISECOND)
        def realVal =  Instant.ofEpochMilli(nanoseconds).atZone(ZoneId.of("Europe/Moscow"))
        return AssertionsTableUtils.FORMAT.format(realVal)
    }

    static def isEquals(List<Map<String, Object>> received, Integer rowNum, String fieldName, def expectedValue){
        def assertionResult = false
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (rowNum >= received.size() || rowNum < 0) {
            receivedMsg = "there isn't row number \"${rowNum}\", correct rows range is [0,${received.size()})"
        } else if (!received.get(rowNum).containsKey(fieldName)) {
            receivedMsg = "there isn't field \"${fieldName}\" in row \"${rowNum}\""
        } else {
            def val = received.get(rowNum).get(fieldName)
            def realVal
            switch (expectedValue.getClass().getSimpleName()) {
                case "String"           :
                    if (val instanceof byte[]) {
                        realVal = Arrays.stream(val).map({ it as char }).collect().join("")
                    } else if (val instanceof SimpleRecord) {
                        realVal = val.toString()
                    } else {
                        realVal = val
                    }
                    break
                case "Integer"          : realVal = val as int; break
                case "Long"             : realVal = val as long; break
                case "Boolean"          : realVal = val as boolean; break
                case "Double"           : realVal = val as double; break
                case "ZonedDateTime"    :
                    expectedValue = AssertionsTableUtils.FORMAT.format(expectedValue)
                    realVal = getDateStrFromBytes(val, expectedValue)
                    break
                case "NullObject"       : realVal = val; break
                default                 : throw new ru.gavr.bdd4hadoop.exceptions.ExpectException("Can not compare type ${expectedValue.getClass().getSimpleName()}")
            }
            assertionResult = realVal == expectedValue
            if (!assertionResult)
                receivedMsg = "received data in row \"$rowNum\" field \"$fieldName\" equals \"$realVal\""
        }
        def expectedMsg = "received data in row \"$rowNum\" field \"$fieldName\" equals \"$expectedValue\""
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def isNotEquals(List<Map<String, Object>> received, Integer rowNum, String fieldName, def expectedValue){
        def assertionResult = false
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (rowNum >= received.size() || rowNum < 0) {
            receivedMsg = "there isn't row number \"${rowNum}\", correct rows range is [0,${received.size()})"
        } else if (!received.get(rowNum).containsKey(fieldName)) {
            receivedMsg = "there isn't field \"${fieldName}\" in row \"${rowNum}\""
        } else {
            def val = received.get(rowNum).get(fieldName)
            def realVal
            switch (expectedValue.getClass().getSimpleName()) {
                case "String"           :
                    if (val instanceof byte[]) {
                        realVal = Arrays.stream(val).map({ it as char }).collect().join("")
                    } else if (val instanceof SimpleRecord) {
                        realVal = val.toString()
                    } else {
                        realVal = val
                    }
                    break
                case "Integer"          : realVal = val as int; break
                case "Long"             : realVal = val as long; break
                case "Boolean"          : realVal = val as boolean; break
                case "Double"           : realVal = val as double; break
                case "ZonedDateTime"    :
                    expectedValue = AssertionsTableUtils.FORMAT.format(expectedValue)
                    realVal = getDateStrFromBytes(val, expectedValue)
                    break
                case "NullObject"       : realVal = val; break
                default                 : throw new ru.gavr.bdd4hadoop.exceptions.ExpectException("Can not compare type ${expectedValue.getClass().getSimpleName()}")
            }
            assertionResult = realVal != expectedValue
            if (!assertionResult)
                receivedMsg = "received data in row \"$rowNum\" field \"$fieldName\" notEquals \"$realVal\""
        }
        def expectedMsg = "received data in row \"$rowNum\" field \"$fieldName\" notEquals \"$expectedValue\""
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def isEmptyStr(List<Map<String, Object>> received, Integer rowNum, String fieldName){
        def assertionResult = false
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (rowNum >= received.size() || rowNum < 0) {
            receivedMsg = "there isn't row number \"${rowNum}\", correct rows range is [0,${received.size()})"
        } else if (!received.get(rowNum).containsKey(fieldName)) {
            receivedMsg = "there isn't field \"${fieldName}\" in row \"${rowNum}\""
        } else {
            def val = received.get(rowNum).get(fieldName)
            def realVal
            if (val instanceof byte[]) {
                realVal = Arrays.stream(val).map({ it as char }).collect().join("")
            } else {
                realVal = val
            }

            assertionResult = (realVal as String).isEmpty()
            if (!assertionResult)
                receivedMsg = "received data in row \"$rowNum\" field \"$fieldName\" is not empty \"$realVal\""
        }
        def expectedMsg = "received data in row \"$rowNum\" field \"$fieldName\" isEmpty"
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def isStartsWith(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValue){
        def assertionResult = false
        def expectedMsg = "parquet record in row \"$rowNum\" field \"$fieldName\" starts with \"$expectedValue\""
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (rowNum >= received.size() || rowNum < 0) {
            receivedMsg = "there isn't row number \"${rowNum}\", correct rows range is [0,${received.size()})"
        } else if (!received.get(rowNum).containsKey(fieldName)) {
            receivedMsg = "there isn't field ${fieldName} in row \"${rowNum}\""
        } else {
            def val = received.get(rowNum).get(fieldName)
            def realVal
            if (val instanceof byte[]) {
                realVal = Arrays.stream(val).map({ it as char }).collect().join("")
            } else {
                realVal = val
            }
            assertionResult = realVal.startsWith(expectedValue)
            if (!assertionResult) {
                if (realVal.length() > expectedValue.length()) {
                    realVal = realVal.substring(0, expectedValue.length())
                }
                receivedMsg = "parquet record in row \"$rowNum\" field \"$fieldName\" starts with \"${realVal}\""
            }
        }
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def isContainsFieldUtils(List<Map<String, Object>> received, String fieldName){
        def assertionResult = false
        def expectedMsg = "received data contains field \"$fieldName\""
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else {
            def res = received.stream()
                    .filter({it.get(fieldName) != null })
                    .find()
            assertionResult = res != null
            if (!assertionResult)
                receivedMsg = "received data does not contain field \"$fieldName\""
        }
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def isEqualsBase64(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValue){
        def assertionResult = false
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (rowNum >= received.size() || rowNum < 0) {
            receivedMsg = "there isn't row number \"${rowNum}\", correct rows range is [0,${received.size()})"
        } else if (!received.get(rowNum).containsKey(fieldName)) {
            receivedMsg = "there isn't field \"${fieldName}\" in row \"${rowNum}\""
        } else {
            try {
                def val = received.get(rowNum).get(fieldName)
                String realVal
                Base64.Decoder decoder = Base64.getDecoder()
                if (val instanceof byte[]) {
                    realVal = new String(decoder.decode(val))
                } else {
                    realVal = new String(decoder.decode(((String)val).getBytes()))
                }
                assertionResult = realVal == expectedValue
                if (!assertionResult)
                    receivedMsg = "received data in row \"$rowNum\" field \"$fieldName\" equals \"$realVal\""
            } catch(Exception ex) {
                receivedMsg = ex.getMessage()
            }
        }
        def expectedMsg = "received decoded base64 data in row \"$rowNum\" field \"$fieldName\" equals \"$expectedValue\""
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def isStartsWithBase64(List<Map<String, Object>> received, Integer rowNum, String fieldName, String expectedValue){
        def assertionResult = false
        def expectedMsg = "parquet decoded base64 record in row \"$rowNum\" field \"$fieldName\" starts with \"$expectedValue\""
        String receivedMsg = ""

        if (received == null || received.isEmpty()) {
            receivedMsg = "received data is empty"
        } else if (rowNum >= received.size() || rowNum < 0) {
            receivedMsg = "there isn't row number \"${rowNum}\", correct rows range is [0,${received.size()})"
        } else if (!received.get(rowNum).containsKey(fieldName)) {
            receivedMsg = "there isn't field ${fieldName} in row \"${rowNum}\""
        } else {
            try {
                def val = received.get(rowNum).get(fieldName)
                String realVal
                Base64.Decoder decoder = Base64.getDecoder()
                if (val instanceof byte[]) {
                    realVal = new String(decoder.decode(val))
                } else {
                    realVal = new String(decoder.decode(((String) val).getBytes()))
                }
                assertionResult = realVal.startsWith(expectedValue)
                if (!assertionResult) {
                    if (realVal.length() > expectedValue.length()) {
                        realVal = realVal.substring(0, expectedValue.length())
                    }
                    receivedMsg = "parquet decoded base64 record in row \"$rowNum\" field \"$fieldName\" starts with \"${realVal}\""
                }
            } catch(Exception ex) {
                receivedMsg = ex.getMessage()
            }
        }
        return [assertionResult, expectedMsg, receivedMsg]
    }
}
