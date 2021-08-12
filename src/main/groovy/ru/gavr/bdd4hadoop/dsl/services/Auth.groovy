package ru.gavr.bdd4hadoop.dsl.services

import groovy.transform.Canonical
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.enums.AuthType

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Auth implements Serializable {
    String principal = ""
    String keytab = ""
    AuthType authType = AuthType.KERBEROS
    String configPath = ""

    @Autowired
    Credentials credentials

    def credentials(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Credentials) Closure closure) {
        closure.setDelegate(credentials)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()
    }
}