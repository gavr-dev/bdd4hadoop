package ru.gavr.bdd4hadoop

import com.google.common.cache.LoadingCache
import ru.gavr.bdd4hadoop.connectors.RunAutotestCommandConnector
import ru.gavr.bdd4hadoop.dsl.Test
import ru.gavr.bdd4hadoop.dsl.operations.Operation
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.enums.ServerMode
import ru.gavr.bdd4hadoop.enums.ServiceType
import ru.gavr.bdd4hadoop.exceptions.ActionResultException
import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class CallableTest {
    Test test
    private Map<String, String> defaultConfigsStorage = new HashMap<String, String>("DEFAULT" : "DEFAULT")

    @Autowired
    TestResult testResult


    def preconditions() {
        log.info("--- Preconditions processing ---")
        if (test.preconditionsList.isEmpty()) {
            log.info("Test preconditions are empty. Nothing to do in \"precondition\" method")
            return
        }
        test.preconditionsList.each {
            checkConditionResult(processAction(it), "Precondition", it.getAction())
        }
    }


    def steps() {
        log.info("--- Steps processing ---")
        if (test.stepsList.isEmpty()) {
            log.info("Test steps are empty. Nothing to do in \"steps\" method")
            return
        }

        def checkDataWithExpectation = { String action, def data, Closure expectMethod ->
            log.debug("Data for check: \"$data\"")
            def (assertionResult, expectedMsg, receivedMsg) = expectMethod(data)
            if (!assertionResult) {
                String exceptionMsg = "Step \"$action\" failed"
                testResult.addResultDetails("$exceptionMsg<br/><br/>Expected: ${expectedMsg}<br/>Received: ${receivedMsg}")
                throw new ActionResultException(exceptionMsg)
            }
        }
        test.stepsList.each {
            def (result, error) = processAction(it)
            log.debug("Checking step result")
            it.getExpect().getExceptionAssertions().entrySet().each { assertionOperationEntry ->
                checkDataWithExpectation(it.getAction(), error, assertionOperationEntry.getValue())
            }
            it.getExpect().getResultAssertions().entrySet().each { assertionOperationEntry ->
                checkDataWithExpectation(it.getAction(), result, assertionOperationEntry.getValue())
            }
        }
    }

    def postconditions() {
        log.info("--- Postconditions processing ---")
        if (test.getPostconditionsList().isEmpty()) {
            log.info("Test postconditions are empty. Nothing to do in \"postcondition\" method")
            return
        }
        test.postconditionsList.each {
            checkConditionResult(processAction(it), "Postcondition", it.getAction())
        }
    }

    def processAction(Operation operation) {
        log.debug("Run \"processAction\" method")
        RunAutotestCommandConnector connector = getConnector(operation.getService())
        return connector.runQuery(operation.action)
    }

    private RunAutotestCommandConnector getConnector(Service service){
        log.debug("Run \"getConnector\" method")
        if (service?.getServiceType()?.getServerMode() == ServerMode.JDBC) {
            return jdbcConnectorPool.get(service)
        }  else if (service?.getServiceType() == ServiceType.HDFS) {
            return hdfsConnectorPool.get(service)
        } else if (service?.getServiceType() == ServiceType.SPARK) {
            return sparkConnectorPool.get(service)
        } else {
            return driverConnectorPool.get(service)
        }
    }

    def checkConditionResult(result, conditionName, action) {
        if (result.get(0).equals("FAILURE") && !result.get(1).equals("NO ERROR") ) {
            String exceptionMsg = "${conditionName} \"${action}\" failed"
            testResult.addResultDetails("$exceptionMsg<br/><br/>Expected: success<br/>Received: ${result.get(1)}")
            log.error(result.get(1))
            throw new ActionResultException(exceptionMsg)
        }
    }

    @Override
    String toString() {
        return  "| ${test.getGroupName()} " +
                "| ${test.getName()} " +
                "| ${testResult.isPassed()} " +
                "| ${test.getDescription()} " +
                "| ${testResult.getActionsMdFormat(test)} " +
                "| ${testResult.getResult()}" +
                "| ${testResult.getResultDetails()} |"
    }

}