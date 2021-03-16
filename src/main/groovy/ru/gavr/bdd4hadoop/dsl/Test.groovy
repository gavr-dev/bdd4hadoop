package ru.gavr.bdd4hadoop.dsl

import ru.gavr.bdd4hadoop.dsl.operations.Operation
import ru.gavr.bdd4hadoop.dsl.services.Services
import groovy.transform.Canonical
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Test implements Serializable {
    String groupName = ""
    String name = ""
    String description = " "
    String testType = ""

    @Autowired
    Services services
    @Autowired
    ObjectProvider<Operation> operationsObjectProvider

    List<Operation> preconditionsList = new ArrayList<>()
    List<Operation> postconditionsList = new ArrayList<>()
    List<Operation> stepsList = new ArrayList<>()
    List<String> stepsComponents = new ArrayList<>()

    def groupName(String groupName) {
        this.groupName = groupName
    }

    def name(String name) {
        this.name = name
    }

    def description(String description) {
        this.description = description
    }

    def services(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Services) Closure closure) {
        closure.setDelegate(services)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()
    }

    def preconditions(Closure closure) {
        addOperationsToList(closure, preconditionsList, false)
    }

    def postconditions(Closure closure) {
        addOperationsToList(closure, postconditionsList, false)
    }

    def steps(Closure closure) {
        addOperationsToList(closure, stepsList, true)
    }

    void addOperationsToList(Closure closure, List<Operation> operationsList, boolean isSteps) {
        List<Closure> list = closure.call() as List<Closure>
        list.each{
            Operation operation = operationsObjectProvider.getObject()
            operation.setOperation(it, isSteps)
            operationsList.add(operation)
        }
    }
}