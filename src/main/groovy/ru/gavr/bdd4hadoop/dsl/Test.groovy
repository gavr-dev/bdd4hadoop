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

}