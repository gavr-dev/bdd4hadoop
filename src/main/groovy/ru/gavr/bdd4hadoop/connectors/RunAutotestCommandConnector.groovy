package ru.gavr.bdd4hadoop.connectors

@FunctionalInterface
interface RunAutotestCommandConnector {
    def runQuery(String query)
}