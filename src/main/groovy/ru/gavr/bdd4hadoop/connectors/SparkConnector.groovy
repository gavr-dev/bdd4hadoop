package ru.gavr.bdd4hadoop.connectors

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.enums.SparkCommandType
import ru.gavr.bdd4hadoop.exceptions.NoSuchCommandException

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.security.PrivilegedExceptionAction

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class SparkConnector implements RunAutotestCommandConnector {

    private Service service
    SparkSession sparkSession
    private SparkContext scContext
    UserGroupInformation clientUgi

    private KerberosConnection kerberosConnection

    @Autowired
    ObjectProvider<KerberosConnection> kerberosConnectionObjectProvider

    static Integer LIMIT = 5

    SparkConnector(Service service) {
        this.service = service
    }

    @PostConstruct
    void init(){
        log.info("Starting \"init\" method")

        Configuration conf = new Configuration(false)
        List<String> configFiles = [
                "/etc/spark/conf/hdfs-site.xml",
                "/etc/spark/conf/core-site.xml",
                "/etc/spark/conf/yarn-site.xml",
                "/etc/spark/conf/hive-site.xml"
        ]
        configFiles.each { conf.addResource(new Path(it)) }

        Configuration sparkSiteConf = new Configuration(false)
        sparkSiteConf.addResource(new Path("/etc/spark/conf/spark-site.xml"))

        SparkConf sparkConf = new SparkConf(false)
        for (Map.Entry<String, String> entry : conf) {
            sparkConf.set("spark.hadoop." + entry.getKey(), entry.getValue())
        }
        for (Map.Entry<String, String> entry : sparkSiteConf) {
            sparkConf.set(entry.getKey(), entry.getValue())
        }

        UserGroupInformation.setConfiguration(SparkHadoopUtil.get().newConfiguration(sparkConf))
        kerberosConnection = kerberosConnectionObjectProvider.getObject(configFiles, service.getAuth().principal, service.getAuth().keytab)
        clientUgi = kerberosConnection.getUgi()

        clientUgi.doAs(new PrivilegedExceptionAction() {
            @Override
            Object run() throws Exception {
                log.info("Start building Spark session")
                Logger.getLogger("org").setLevel(Level.ERROR)
                sparkSession = SparkSession.builder()
                        .config(sparkConf)
                        .enableHiveSupport()
                        .getOrCreate()
                log.info("Session is built")
                scContext = sparkSession.sparkContext()
                return null
            }
        } )
    }

    def readHdfsTextFile(String file){
        log.info("Starting \"getHdfsTextFile\" method")
        String lines = sparkSession.read().textFile(file).head()
        log.info("Ending \"getHdfsTextFile\" method")
        return lines
    }

    def readHdfsParquetFile(String file){
        log.info("Starting \"getHdfsParquetFile\" method")
        Dataset<Row> rows = sparkSession.read().parquet(file)

        List<String> names = rows.columns().toList()
        List<Row> rowsList = rows.takeAsList(LIMIT)
        List<Map<String, Object>> resultList = new ArrayList<>()
        for (int i = 0; i < rowsList.size(); i++){
            Map<String, Object> rowMap = new HashMap<>()
            for (int j = 0; j < names.size(); j++){
                rowMap.put(names[j], rowsList[i].apply(j))
            }
            resultList.add(rowMap)
        }

        log.info("Ending \"getHdfsParquetFile\" method")
        return resultList
    }

    def runSqlCommand(String command){
        log.info("Starting \"runSqlCommand\" method")
        Dataset<Row> rows = sparkSession.sql(command)
        List<String> names = rows.columns().toList()
        List<Row> rowsList = rows.takeAsList(LIMIT)
        List<Map<String, Object>> resultList = new ArrayList<>()
        for (int i = 0; i < rowsList.size(); i++){
            Map<String, Object> rowMap = new HashMap<>()
            for (int j = 0; j < names.size(); j++){
                rowMap.put(names[j], rowsList[i].apply(j))
            }
            resultList.add(rowMap)
        }
        log.info("Ending \"runSqlCommand\" method")
        return resultList
    }

    @Override
    def runQuery(String query) {
        def result = ["FAILURE", "SUCCESS"]
        log.info("Execute \"${query}\"")
        String[] params = query.split(" ")
        String commandType = params[0]
        String paramValue = params[1]

        def commands = []

        SparkCommandType.values().each {
            commands << it.getCommandValue()
        }

        if(!commands.contains(commandType)) {
            throw new NoSuchCommandException("${commandType}")
        }

        try {
            switch (commandType) {
                case SparkCommandType.READ_PARQUET.getCommandValue():
                    List<Map<String, Object>> resultList = readHdfsParquetFile(paramValue)
                    result = [resultList, "SUCCESS"]
                    break
                case SparkCommandType.READ_TXT.getCommandValue():
                    String firstLine = readHdfsTextFile(paramValue)
                    result = [firstLine, "SUCCESS"]
                    break
                case SparkCommandType.SQL.getCommandValue():
                    String sqlCommand = query.substring(query.indexOf(' ') + 1)
                    List<Map<String, Object>> resultList = runSqlCommand(sqlCommand)
                    result = [resultList, "SUCCESS"]
                    break
            }
        } catch (AnalysisException e) {
            result = ["FAILURE", e.toString()]
        } catch (Exception e) {
            result = ["FAILURE", e.toString()]
        }
        return result
    }

    @PreDestroy
    void destroy(){
        log.info("Started to turn off the Spark connector")
        sparkSession.close()
        log.info("Turned off the Spark connector")
    }
}
