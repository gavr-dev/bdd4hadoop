package ru.gavr.bdd4hadoop.enums

enum TestType implements Serializable {

    HIVE(typeName: "HIVE"),
    HDFS(typeName: "HDFS")


    String typeName

    TestType(def map){
        this.typeName = map["typeName"] as String
    }
}