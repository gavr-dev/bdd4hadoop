package ru.gavr.bdd4hadoop.enums

enum TestType implements Serializable {

    HIVE(typeName: "HIVE"),
    SPARK_WITH_HIVE(typeName: "SPARK_WITH_HIVE"),
    SPARK(typeName: "SPARK"),
    HDFS(typeName: "HDFS")


    String typeName

    TestType(def map){
        this.typeName = map["typeName"] as String
    }
}