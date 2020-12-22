package ru.gavr.bdd4hadoop.dsl.services

import ru.gavr.bdd4hadoop.enums.ServiceType
import groovy.transform.Canonical
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Services implements Serializable{
    List<Service> serviceList = new ArrayList<>()

    @Autowired
    ObjectProvider<Service> serviceObjectProvider



}
