package ru.gavr.bdd4hadoop.enums

enum ServiceType implements Serializable {
    HIVESERVER2(serverMode: ServerMode.JDBC, componentName: "HS2"),
    HIVEMETASTORE(serverMode: ServerMode.METASTORE, componentName: "HMS"),
    HDFS(serverMode: ServerMode.CLIENT, componentName: "HDFS"),
    SPARK(serverMode: ServerMode.CLIENT, componentName: "SPARK")

    ServerMode serverMode
    String componentName


    ServiceType(def map) {
        this.serverMode = map["serverMode"] as ServerMode
        this.componentName = map["componentName"] as String
    }

    ServiceType(ServerMode serverMode, String componentName) {
        this.serverMode = serverMode
        this.componentName = componentName
    }
}