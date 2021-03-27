package ru.gavr.bdd4hadoop.connectors.utils

class ResultObject implements GroovyInterceptable {
    def storage = [:]
    def propertyMissing(String name, value) {
        storage[name] = value
    }
    def propertyMissing(String name) {
        storage[name]
    }
}