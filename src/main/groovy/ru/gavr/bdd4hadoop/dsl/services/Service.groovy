package ru.gavr.bdd4hadoop.dsl.services

import ru.gavr.bdd4hadoop.enums.ServiceType
import groovy.transform.Canonical
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Service implements Serializable {
    String id = ""
    ServiceType serviceType

    @Autowired
    Config config
    @Autowired
    Auth auth
    @Autowired
    Auth proxyuser


    def config(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Config) Closure closure) {
        closure.setDelegate(config)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()
    }

    def auth(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Auth) Closure closure) {
        closure.setDelegate(auth)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()
    }

    def proxyuser(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Auth) Closure closure) {
        closure.setDelegate(proxyuser)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()
    }
}