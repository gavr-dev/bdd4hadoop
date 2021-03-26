package ru.gavr.bdd4hadoop.enums

enum HDFSCommandType implements Serializable {

    TOUCHZ(commandType: "hadoop_touchz"),
    CREATE_DIRECTORY(commandType: "hadoop_mkdirs"),
    DELETE(commandType: "hadoop_rm"),
    DOWNLOAD(commandType: "hadoop_get"),
    READ(commandType: "parquet_read"),
    COUNT_PARQUET_ROWS(commandType: "parquet_count_rows"),
    READ_FILE_LINES(commandType: "read_file_lines"),
    UPLOAD(commandType: "hadoop_put"),
    LIST(commandType: "hadoop_ls"),
    CHMOD(commandType: "hadoop_chmod"),
    CHOWN(commandType: "hadoop_chown"),
    CHANGE_PARAM(commandType: "change_param"),
    SLEEP(commandType: "sleep"),
    MOVE(commandType: "hadoop_mv"),
    COPY(commandType: "hadoop_cp"),
    DIFF(commandType: "diff"),
    CREATE_FILE(commandType: "hadoop_create_file")

    private String commandType

    HDFSCommandType(def map){
        this.commandType = map["commandType"] as String
    }

    String getCommandValue(){
        return this.commandType
    }

}
