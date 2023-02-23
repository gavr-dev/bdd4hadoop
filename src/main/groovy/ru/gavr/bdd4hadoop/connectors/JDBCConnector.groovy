package ru.gavr.bdd4hadoop.connectors

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.UserGroupInformation
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.dsl.services.Config
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.exceptions.ConnectorException

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.security.PrivilegedExceptionAction
import java.sql.*

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class JDBCConnector implements RunAutotestCommandConnector {

    private final String driverName = "org.apache.hive.jdbc.HiveDriver"
    private Service service
    private Connection connection
    private String connection_string
    private HiveConf connectorConf
    private Configuration hadoopConf = new Configuration(false)
    private UserGroupInformation userGroupInformation
    private KerberosConnection kerberosConnection

    @Autowired ObjectProvider<KerberosConnection> kerberosConnectionObjectProvider


    JDBCConnector(Service service) {
        this.service = service
    }

    @PostConstruct
    void init(){
        createConnectionString()
        if (service.getProxyuser() == null || service.getProxyuser().getPrincipal() == "" || service.getProxyuser().getPrincipal() == "") {
            log.info("Creation origin kerberosConnection")
            kerberosConnection = kerberosConnectionObjectProvider.getObject(
                    service.getAuth().getPrincipal(),
                    service.getAuth().getKeytab(),
                    "thrift://0.0.0.0:9083"
            )
            userGroupInformation = kerberosConnection.getUgi()
        } else {
            log.info("Creation kerberosConnection with proxyuser = ${service.getProxyuser().toString()}")
            kerberosConnection = kerberosConnectionObjectProvider.getObject(
                    service.getProxyuser().getPrincipal(),
                    service.getProxyuser().getKeytab(),
                    service.getAuth().getPrincipal(),
                    "thrift://0.0.0.0:9083"
            )
        }
        hadoopConf = kerberosConnection.getConf()
        connectorConf = new HiveConf(hadoopConf, JDBCConnector.class)

        connection = initSecuredConnection(connection_string)
        log.info("JDBC CONNECTOR SUCCESSFULLY CONSTRUCT")
    }


    private def createConnectionString() {
        Config connectedService = service.getConfig()
        StringBuilder connection_string_builder = new StringBuilder()
        connection_string_builder.append("jdbc:hive2://")
        connection_string_builder.append(connectedService?.getHostname() ?: "0.0.0.0")
        connection_string_builder.append(":")
        connection_string_builder.append(connectedService?.getPort() ?: "10000")
        connection_string_builder.append("/")

        String principal = connectedService.getConnectionPrincipal()?:"hive/_HOST@REALM"
        connection_string_builder.append(";principal=" + principal)
        connection_string = connection_string_builder.toString()
        log.info("connect via jdbc is [$connection_string]")
    }

    Connection initSecuredConnection(String connection_string, UserGroupInformation ugi = userGroupInformation) throws Exception {
        (Connection) ugi.doAs(
                new PrivilegedExceptionAction<Object>() {
                    Object run() {
                        initConnection(connection_string)
                    }
                })
    }

    Connection initConnection(String connection_string) throws Exception {
        Class.forName(driverName)
        Connection con = null
        String JDBC_DB_URL = "${connection_string}"
        def waitTimeSec = 10
        while (true) {
            try {
                con = DriverManager.getConnection(JDBC_DB_URL)
                break
            } catch (SQLException e) {
                if(waitTimeSec-- > 0){
                    log.info("Waiting hiveServer2 ${waitTimeSec}: ${e.getMessage()}")
                    sleep(1000)
                    continue
                }
                throw new ConnectorException("Error init connect!", e)
            } catch (ClassNotFoundException e) {
                throw new ConnectorException("There is not JDBC DriverClass", e)
            }
        }

        return con
    }



    def runQuery(String query) {
        Statement stmt = connection.createStatement()
        ResultSet rs
        String exceptionMessage = "NO ERROR"
        log.info("Execute \"${query}\"")

        try {
            if (stmt.execute(query))
                rs = stmt.getResultSet()
        } catch (SQLException e) {
            exceptionMessage = e.getMessage()
        }
        if(query.toLowerCase().contains("select ") && rs == null){
            def msg = "ERROR RESULT_SET ${query} - is NULL"
            log.error(msg)
            printLogExecuting(exceptionMessage)
            return ["ERROR", msg]
        }
        if (rs == null) {
            printLogExecuting(exceptionMessage)
            return ["NO DATA", exceptionMessage]
        }
        ResultSetMetaData rsmd = rs.getMetaData()
        int columnsNumber = rsmd.getColumnCount()
        List<Map<String, Object>> resultList = new ArrayList<>()
        while (rs.next()) {
            Map<String, Object> rowMap = new HashMap<>()
            for (int i = 1; i <= columnsNumber; i++) {
                rowMap.put(rsmd.getColumnName(i), rs.getString(i))
            }
            resultList.add(rowMap)
        }

        printLogExecuting(exceptionMessage)
        return [resultList, exceptionMessage]
    }

    static def printLogExecuting(exceptionMessage) {
        if (!exceptionMessage.equals("NO ERROR")) {
            log.info("Result executing: failure")
        } else {
            log.info("Result executing: success")
        }
    }

    @PreDestroy
    void destroy() {
        connection.close()
        log.info("JDBC CONNECTOR SUCCESSFULLY DESTROY")
    }


}
