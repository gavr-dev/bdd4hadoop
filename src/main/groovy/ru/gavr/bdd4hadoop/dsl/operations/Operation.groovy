package ru.gavr.bdd4hadoop.dsl.operations

import dev.gavr.bdd4hadoop.dsl.services.Service
import dev.gavr.bdd4hadoop.exceptions.ExpectException
import groovy.transform.Canonical
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Operation implements Serializable{
    String serviceId = ""
    String action = ""

    @Autowired
    Expect expect
    @Autowired
    Service service

    def setOperation(def map, def isSteps) {
        if (map.containsKey("serviceId")) {
            this.serviceId = map["serviceId"]
        }
        if (map.containsKey("action")) {
            this.action = map["action"]
        }
        if (map.containsKey("expect") && isSteps) {
            def closure = map["expect"] as Closure
            closure.setDelegate(expect)
            closure.setResolveStrategy(Closure.DELEGATE_FIRST)
            closure.call()
        } else if (!map.containsKey("expect") && isSteps) {
            throw new ExpectException("Steps can not be without \"expect\"")
        } else if (map.containsKey("expect") && !isSteps) {
            throw new ExpectException("Conditions can not be with \"expect\"")
        }

    }
 }
