<h1 align="center">Bdd4Hadoop</h1>
<p align="center">
<img src="https://img.shields.io/github/last-commit/gavr-dev/bdd4hadoop"/>
<a href="https://github.com/gavr-dev/bdd4hadoop/tags" alt="Tag"><img src="https://img.shields.io/github/v/tag/gavr-dev/bdd4hadoop"/></a>
<a href="https://github.com/gavr-dev/bdd4hadoop/blob/main/LICENSE" alt="GPLv3 licensed"><img src="https://img.shields.io/badge/license-GPLv3-blue"/></a>
</p>

**Bdd4Hadoop** is a framework designed to create behaviour-driven development-style automated tests specifically for **Apache Hadoop**.
It includes a custom domain-specific language (**DSL**) based on Groovy, facilitating test writing.
Additionally, the framework features connectors for integrating with the core components of Apache Hadoop.

## DSL
### Test template
```groovy
template {
    testType = "" //HDFS, HIVE, SPARK
    groupName = ""
    services {
        service_name {
            id = ""
            auth {
                authType = "" //PLAIN, KERBEROS
                principal = ""
                keytab = ""
                configPath = ""
            }
            config {
                connectionPrincipal = ""
                hostname = ""
                port = 0
            }
        }
    }
}
```
### Test
```groovy
test {
    name ""
    preconditions {[
            [ serviceId: "service_name", action: ""],
            [ serviceId: "service_name", action: ""],
    ]}
    steps {[
            [ serviceId: "service_name", 
              action: "",
              expect: {
                  result()
                  exception() 
              }
            ]
    ]}
    postconditions {[
            [ serviceId: "service_name", action: ""],
            [ serviceId: "service_name", action: ""],
    ]}
}
```

## Connectors
### HDFS
```groovy
    TOUCHZ(commandType: "hadoop_touchz")
    CREATE_DIRECTORY(commandType: "hadoop_mkdirs")
    DELETE(commandType: "hadoop_rm")
    DOWNLOAD(commandType: "hadoop_get")
    READ(commandType: "parquet_read")
    COUNT_PARQUET_ROWS(commandType: "parquet_count_rows")
    READ_FILE_LINES(commandType: "read_file_lines")
    UPLOAD(commandType: "hadoop_put")
    LIST(commandType: "hadoop_ls")
    CHMOD(commandType: "hadoop_chmod")
    CHOWN(commandType: "hadoop_chown")
    SLEEP(commandType: "sleep")
    MOVE(commandType: "hadoop_mv")
    COPY(commandType: "hadoop_cp")
    DIFF(commandType: "diff")
    CREATE_FILE(commandType: "hadoop_create_file")
```
### Hive
```groovy
    SQL(commandType: "sql")
```
### Spark
```groovy
    READ_TXT(commandType: "text_read")
    READ_PARQUET(commandType: "parquet_read")
    SQL(commandType: "sql_command")
```

## Build
To assemble, just do ```mvn clean install```

## Run
After building the project, an archive ```bdd4hadoop-<version>.zip``` is created, which contains:
- bin/start.sh
- bdd4hadoop.jar

Command to run tests:
```shell
./start.sh --component=HDFS,HIVE,SPARK --tests=/opt/tests --result=/opt/results
```