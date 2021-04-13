package ru.gavr.bdd4hadoop.enums

enum SparkCommandType implements Serializable{

    READ_TXT(commandType: "text_read"),
    READ_PARQUET(commandType: "parquet_read"),
    SQL(commandType: "sql_command")

    private String commandType

    SparkCommandType(def map){
        this.commandType = map["commandType"] as String
    }

    String getCommandValue(){
        return this.commandType
    }
}