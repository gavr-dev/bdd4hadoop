package ru.gavr.bdd4hadoop.dsl

import ru.gavr.bdd4hadoop.CallableTest
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.exceptions.ServiceIdException
import groovy.util.logging.Commons
import org.apache.commons.lang.SerializationUtils
import org.springframework.beans.factory.ObjectFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import java.util.stream.Collectors

@Component
@Commons
class TestBuilder {

    @Autowired
    ObjectFactory<Test> testObjectFactory
    @Autowired
    ObjectFactory<CallableTest> callableTestObjectFactory


    Test templateTest
    boolean templated = false

    void test(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Test) Closure closure) {
        log.debug("Test processing")
        Test test = testObjectFactory.getObject()
        if (templated) {
            test = (Test) SerializationUtils.clone(templateTest)

        }
        CallableTest callableTest = callableTestObjectFactory.getObject()
        closure.setDelegate(test)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)

        try {
            closure.call()
            checkServiceId(test)
            callableTest.getTestResult().setIsRead(true)
        } catch(Throwable e){
            String msg = "Test \"${test.getName()}\" failed to parse: ${e}".replaceAll("\n", ". ")
            log.error(msg)
            callableTest.getTestResult().setResultDetails(msg)
        } finally {
            callableTest.setTest(test)
        }
    }

    void template(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Test) Closure closure) {
        log.debug("Template processing")
        templateTest = testObjectFactory.getObject()
        closure.setDelegate(templateTest)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()
        checkServiceId(templateTest)
        templated = true
    }

    static def checkServiceId(Test test) {
        def check = { list, blockname ->
            list.each {
                String serviceId = it.serviceId
                if (serviceId.isEmpty())
                    throw new ServiceIdException("ServiceId is empty in ${blockname}")
                List<Service> suitableServices = test.getServices().getServiceList().stream()
                        .filter({it.id.equals(serviceId)})
                        .collect(Collectors.toList())
                if (suitableServices.isEmpty())
                    throw new ServiceIdException("Can not find serviceId \"${serviceId}\" for ${blockname}")
                it.setService(suitableServices[0])
            }
        }
        check(test.getPreconditionsList(), "precondition")
        check(test.getPostconditionsList(), "postcondition")
        check(test.getStepsList(), "step")
        addStepComponents(test)
    }

    static def addStepComponents(Test test) {
        test.getStepsList().stream().each {
            test.stepsComponents.add(it.getService().getServiceType().getComponentName())
        }
    }
}