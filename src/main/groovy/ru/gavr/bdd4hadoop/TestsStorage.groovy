package ru.gavr.bdd4hadoop

import groovy.util.logging.Commons
import org.springframework.stereotype.Component

import java.util.stream.Collectors

@Component
@Commons
class TestsStorage {
    List<CallableTest> tests = new LinkedList<>()

    List<CallableTest> getAllTest() {
        return getReadTests()
    }

    List<CallableTest> getTestsByComponents(List<String> components) {
        tests = tests.stream()
                .filter({
                    it.getTest().getTestType().isEmpty() || components.contains(it.getTest().getTestType())})
                .collect(Collectors.toList())
        return getReadTests()
    }

    private List<CallableTest> getReadTests() {
        List<CallableTest> readTest = tests.stream()
                .filter({it.getTestResult().isRead})
                .collect(Collectors.toList())
        testUnreadCounter = tests.size() - readTest.size()
        return readTest
    }

    int testUnreadCounter = 0
    List<String> unReadableFiles = []

    void printResultsFormatted(String resultPath) {
        int testsPassedCounter = 0
        int testsFailedCounter = 0
        int testsNotExecutedCounter = 0

        new File("${resultPath}").mkdir()
        File file = new File("${resultPath}/results.md")
        file.write("| Group | Name | Passed | Description | Actions | Result | Result Details | \n")
        file.append( "| :----: | :----: | :----: | :----: | :---- | :----: | :---- |\n")

        tests.each { test ->
            file.append("$test\n")
            if (test.getTestResult().isRead) {
                if (test.getTestResult().isPassed)
                    testsPassedCounter++
                else if (test.getTestResult().isExecuted)
                    testsFailedCounter++
                else
                    testsNotExecutedCounter++
            }
        }

        file.append("\n<<<<<<< RESULTS >>>>>>>\n\n")
        log.info("Success: $testsPassedCounter \tFail: $testsFailedCounter \tUnread: $testUnreadCounter \tNottested: $testsNotExecutedCounter \t\tTotal: ${tests.size()}")
        file.append(" Success | Fail | Unread | Nottested | Total \n")
        file.append(" :----: | :----: | :----: | :----: |:----: \n")
        file.append(" $testsPassedCounter | $testsFailedCounter | $testUnreadCounter | $testsNotExecutedCounter | ${tests.size()} \n")

        if (unReadableFiles.size() > 0) {
            log.info("Unreadable files: $unReadableFiles")
            file.append("\nUnreadable files: \n")
            file.append("$unReadableFiles\n")
        }
    }
}