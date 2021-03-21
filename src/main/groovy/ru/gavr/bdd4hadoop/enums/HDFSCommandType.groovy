package ru.gavr.bdd4hadoop.enums

enum HDFSCommandType implements Serializable {

    TOUCHZ(commandType: "hadoop_touchz"),
    CREATE_DIRECTORY(commandType: "hadoop_mkdirs"),
    DELETE(commandType: "hadoop_rm"),
    DOWNLOAD(commandType: "hadoop_get"),


    private String commandType

    HDFSCommandType(def map){
        this.commandType = map["commandType"] as String
    }

    String getCommandValue(){
        return this.commandType
    }

}
