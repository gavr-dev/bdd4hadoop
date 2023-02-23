package ru.gavr.bdd4hadoop.assertions

import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class Assertions {
    static def checkResult(Boolean assertionResult, String expectedMsg, String receivedMsg) {
        if (assertionResult) {
            log.info("Expected: $expectedMsg")
            log.info("Received: $expectedMsg")
        } else {
            log.error("Expected: $expectedMsg")
            log.error("Received: $receivedMsg")
        }
        return [assertionResult, expectedMsg, receivedMsg]
    }

    static def equalsTo(String loginfo, String expected, String received) {
        def assertionResult = (expected == received) as Boolean
        return checkResult(assertionResult, "${loginfo} equalsTo \"$expected\"", "${loginfo} equalsTo \"$received\"")
    }

    static def isContains(String loginfo, String expected, String received) {
        def assertionResult = (received =~ expected).find() as Boolean
        return checkResult(assertionResult, "${loginfo} contains \"$expected\"", "${loginfo} contains \"$received\"")
    }

    static def isNotContains(String loginfo, String expected, String received) {
        def assertionResult = !((received =~ expected).find()) as Boolean
        return checkResult(assertionResult, "${loginfo} not contains \"$expected\"", "${loginfo} contains \"$expected\"")
    }

    static def isEmpty(String received) {
        def assertionResult = received == "\"\"" || received == "[]"
        return checkResult(assertionResult, "result is empty", "result $received is not empty")
    }

    static def isNothing(String received) {
        def assertionResult = received == "\"NO DATA\""
        return checkResult(assertionResult, "result is nothing", "result $received is not nothing")
    }

    static def containsLine(List<String> allLines, int lineNum, String expectedLine) {
        def receivedLine = allLines.size() >= lineNum ? allLines.get(lineNum) : "null"
        def assertionResult = receivedLine == expectedLine
        return checkResult(assertionResult, expectedLine, receivedLine)
    }

    static def lineStartWith(List<String> allLines, int lineNum, String expectedLine) {
        def receivedLine = allLines.size() >= lineNum ? allLines.get(lineNum) : "null"
        def assertionResult = receivedLine.startsWith(expectedLine)
        return checkResult(assertionResult, expectedLine, receivedLine)
    }
}
