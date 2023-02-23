package ru.gavr.bdd4hadoop

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.gavr.bdd4hadoop.connectors.DriverConnector
import ru.gavr.bdd4hadoop.connectors.JDBCConnector
import ru.gavr.bdd4hadoop.connectors.SparkConnector
import ru.gavr.bdd4hadoop.connectors.HDFSConnector
import ru.gavr.bdd4hadoop.dsl.services.Service

@Configuration
class BeanConfiguration {

    @Bean
    Binding binding(){
        def binding = new Binding([
                "name": "value",
        ])
        binding
    }

    @Bean
    @Autowired
    LoadingCache<Service, DriverConnector> driverConnectorPool(ObjectProvider<DriverConnector> connectorObjectProvider) {
        CacheLoader<Service, DriverConnector> loader
        loader = new CacheLoader<Service, DriverConnector>() {
            @Override
            DriverConnector load(Service service) {
                return connectorObjectProvider.getObject(service)
            }
        }
        CacheBuilder.newBuilder()
                .initialCapacity(20)
                .concurrencyLevel(10)
                .build(loader)
    }


    @Bean
    @Autowired
    LoadingCache<Service, JDBCConnector> beelineConnectorPool(ObjectProvider<JDBCConnector> connectorObjectProvider) {
        CacheLoader<Service, JDBCConnector> loader
        loader = new CacheLoader<Service, JDBCConnector>() {
            @Override
            JDBCConnector load(Service service) {
                return connectorObjectProvider.getObject(service)
            }
        }
        CacheBuilder.newBuilder()
                .initialCapacity(20)
                .concurrencyLevel(10)
                .build(loader)
    }


    @Bean
    @Autowired
    LoadingCache<Service, HDFSConnector> hdfsConnectorPool(ObjectProvider<HDFSConnector> connectorObjectProvider){

        CacheLoader<Service, HDFSConnector> loader
        loader = new CacheLoader<Service, HDFSConnector>() {
            @Override
            HDFSConnector load(Service service) throws Exception {
                return connectorObjectProvider.getObject(service)
            }
        }
        CacheBuilder.newBuilder()
                .initialCapacity(20)
                .concurrencyLevel(10)
                .build(loader)

    }


    @Bean
    @Autowired
    LoadingCache<Service, SparkConnector> sparkConnectorPool(ObjectProvider<SparkConnector> connectorObjectProvider){

        CacheLoader<Service, SparkConnector> loader
        loader = new CacheLoader<Service, SparkConnector>() {
            @Override
            SparkConnector load(Service service) throws Exception {
                return connectorObjectProvider.getObject(service)
            }
        }
        CacheBuilder.newBuilder()
                .initialCapacity(20)
                .concurrencyLevel(10)
                .build(loader)

    }
}