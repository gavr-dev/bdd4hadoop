package ru.gavr.bdd4hadoop.connectors

import groovy.json.JsonOutput
import groovy.json.JsonParserType
import groovy.json.JsonSlurper
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.connectors.utils.ResultObject
import ru.gavr.bdd4hadoop.dsl.services.Config
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.enums.AuthType

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.security.PrivilegedExceptionAction

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class DriverConnector implements RunAutotestCommandConnector {

    private Driver driver
    private HiveConf hiveConf
    private Service service
    private SessionState driverSessionState
    private UserGroupInformation clientUgi

    private Configuration hadoopConf = new Configuration(false)
    private KerberosConnection kerberosConnection
    private CommandProcessorResponse cpr
    private List<String> hmsResults = new LinkedList()
    private String connection_string


    DriverConnector(Service service) {
        this.service = service
    }

    @Autowired
    ObjectProvider<KerberosConnection> kerberosConnectionObjectProvider

    private def createConnectionString() {
        Config connectedService = service.getConfig()
        StringBuilder connection_string_builder = new StringBuilder()
        connection_string_builder.append("thrift://")
        connection_string_builder.append(connectedService?.getHostname() ?: "0.0.0.0")
        connection_string_builder.append(":")
        connection_string_builder.append(connectedService?.getPort() ?: "9083")
        connection_string = connection_string_builder.toString()
        log.info("create connection to [$connection_string]")
    }

    @PostConstruct
    void construct() {
        createConnectionString()
        if(service.getAuth().getAuthType() == AuthType.KERBEROS) {
            kerberosConnection = kerberosConnectionObjectProvider.getObject(
                    service.getAuth().getPrincipal(),
                    service.getAuth().getKeytab(),
                    connection_string
            )
        } else {
            kerberosConnection = kerberosConnectionObjectProvider.getObject(false)
        }

        hadoopConf = kerberosConnection.getConf()
        hiveConf = new HiveConf(hadoopConf, Driver.class)
        clientUgi = kerberosConnection.getUgi()
        log.info("SUCCESSFULLY CONSTRUCT")

    }

    def runQuery(String query) {
        log.info("Execute ${query}")
        def result = ["NO DATA", "NO ERROR"]
        clientUgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            Void run() throws Exception {
                String queryString
                driver = new Driver(hiveConf)
                try {
                    SessionState.start(new CliSessionState(hiveConf))
                }catch(Exception e){
                    log.error("error from session start.sh: ${e}")
                    result = ["NO DATA", e.getMessage()]
                    return
                }
                try {
                    cpr = driver.run(query)
                    if (cpr.getResponseCode() != 0) {
                        String errMsg = cpr.getErrorMessage()
                        log.error("error from query: [${errMsg}]")
                        result = ["NO DATA", errMsg]
                        return
                    }
                } catch (Throwable e) {
                    log.error(e)
                    log.error("exception while sending query: [${e}]")
                    result = ["NO DATA", e]
                    return
                }
                driver.getResults(hmsResults)
                Schema schema = driver.getSchema()
                if (schema == null || schema.getFieldSchemasSize() == 0) {
                    queryString = hmsResults[0] ?: "NO DATA"
                } else {
                    queryString = transformResultFromPlainToJsonUsingSchema(hmsResults, schema)
                }
                result = [queryString, "NO ERROR"]
                log.info("result is ${queryString}")
            }
        }
        )
        log.info(" return ${result}")
        return result
    }

    static String transformResultFromPlainToJsonUsingSchema(List<String> hmsResults, Schema schema) {
        List<FieldSchema> fieldSchemaList = schema.getFieldSchemas()
        def columnsNumber = schema.getFieldSchemasSize()
        List<ResultObject> resultObjects = new LinkedList<>()
        hmsResults
                .stream()
                .forEach({ it ->
                    ResultObject resultObject = new ResultObject()
                    for (int i = 0; i < columnsNumber; i++) {
                        String columnValue = it
                        resultObject.setProperty(fieldSchemaList.get(i).getName(), columnValue)
                    }
                    resultObjects << resultObject
                })
        JsonOutput.toJson(resultObjects)
    }

    static String transformResultFromJsonToJsonUsingSchema(String hmsResults, schema) {
        def jsonSlurper = new JsonSlurper(type: JsonParserType.LAX)
        List<FieldSchema> fieldSchemaList = schema.getFieldSchemas()
        def columnsNumber = schema.getFieldSchemasSize()
        List<ResultObject> resultObjects = new LinkedList<>()
        ((Map<String, List>) jsonSlurper.parseText(hmsResults))
                .values()
                .toList()
                .get(0)
                .stream()
                .forEach({ it ->
                    ResultObject resultObject = new ResultObject()
                    for (int i = 0; i < columnsNumber; i++) {
                        String columnValue = it
                        resultObject.setProperty(fieldSchemaList.get(i).getName(), columnValue)
                    }
                    resultObjects << resultObject
                })
        JsonOutput.toJson(resultObjects)
    }


    @PreDestroy
    void destroy() {
        driver.close()
        SessionState.detachSession()
        SessionState.get().close()
        driverSessionState.close()
        log.info("DESTROYED")
    }
}
