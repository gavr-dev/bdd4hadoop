package ru.gavr.bdd4hadoop

import ru.gavr.bdd4hadoop.dsl.Test
import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class TestResult {
    String resultDetails = ""
    boolean isPassed = false
    boolean isRead = false
    boolean isExecuted = false
    boolean hasCriticalError = false

    def isPassed(){
        def color = isPassed ? "green" : "red"
        return "<span style='color:$color'>${getIsPassed()}</span>"
    }

    static String getActionsMdFormat(Test test) {
        String mdFormat = ""
        def convertActions = { actionsList, actionsGroupName ->
            if (!actionsList.isEmpty()) {
                mdFormat += "$actionsGroupName <ul>"
                actionsList.each {
                    mdFormat += "<li> " + it.getAction() + "</li>"
                }
                mdFormat += "</ul>"
            }
        }
        convertActions(test.getPreconditionsList(), "Preconditions")
        convertActions(test.getStepsList(), "Steps")
        convertActions(test.getPostconditionsList(), "Postconditions")
        return mdFormat
    }

    String getResult() {
        if (!isRead || !isExecuted || hasCriticalError) {
            return "ERROR"
        } else if (isPassed) {
            return "SUCCESS"
        } else {
            return "FAILED"
        }
    }

    def addResultDetails(String detail) {
        if (resultDetails.isEmpty()) {
            resultDetails = detail
        } else {
            resultDetails += "<br/><br/>$detail"
        }
    }

    String getResultDetails() {
        if (!isExecuted && isRead) {
            return "Not tested"
        }
        return resultDetails
    }


    @Override
    public String toString() {
        return "TestResult{" +
                "resultDetails='" + resultDetails + '\'' +
                ", isPassed=" + isPassed +
                ", isRead=" + isRead +
                ", isExecuted=" + isExecuted +
                ", hasCriticalError=" + hasCriticalError +
                '}';
    }
}
