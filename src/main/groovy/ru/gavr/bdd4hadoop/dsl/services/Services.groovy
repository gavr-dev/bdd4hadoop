package ru.gavr.bdd4hadoop.dsl.services

import groovy.transform.Canonical
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.enums.ServiceType

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Services implements Serializable{
    List<Service> serviceList = new ArrayList<>()

    @Autowired
    ObjectProvider<Service> serviceObjectProvider

    def hdfs(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Service) Closure closure) {
        serviceProcessing(closure, ServiceType.HDFS)
    }

    def hivemetastore(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Service) Closure closure) {
        serviceProcessing(closure, ServiceType.HIVEMETASTORE)
    }

    def hiveserver2(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Service) Closure closure) {
        serviceProcessing(closure, ServiceType.HIVESERVER2)
    }

    def spark(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Service) Closure closure) {
        serviceProcessing(closure, ServiceType.SPARK)
    }

    def serviceProcessing(Closure closure, ServiceType serviceType) {
        Service service  = serviceObjectProvider.getObject()
        service.setServiceType(serviceType)
        closure.setDelegate(service)
        closure.setResolveStrategy(Closure.DELEGATE_FIRST)
        closure.call()

        serviceList.removeIf({ it.id.equals(service.id) })
        serviceList.add(service)

    }

}
