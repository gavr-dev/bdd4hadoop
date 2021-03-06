package ru.gavr.bdd4hadoop.enums

enum TestType implements Serializable {

    HDFS(typeName: "HDFS")

    String typeName

    TestType(def map){
        this.typeName = map["typeName"] as String
    }
}