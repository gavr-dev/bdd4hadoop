package ru.gavr.bdd4hadoop.dsl.services;

import groovy.transform.Canonical
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Credentials implements Serializable {
    String userName
    String userPassword
}