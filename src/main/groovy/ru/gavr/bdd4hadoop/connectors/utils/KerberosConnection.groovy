package ru.gavr.bdd4hadoop.connectors.utils

import groovy.io.FileType
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.exceptions.ConnectorException

import java.nio.file.Path

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class KerberosConnection {
    UserGroupInformation ugi
    Configuration conf

    KerberosConnection() {
    }



    static Path getRealFile(String file1) throws IOException {
        return new File(file1).toPath().toRealPath()
    }

    static KerberosConnection createForService(Service service, ObjectProvider<KerberosConnection> kerberosConnectionObjectProvider) throws ConnectorException {
        if (!service.getAuth().configPath.isEmpty()) {
            def configFiles = []
            def dir = new File(service.getAuth().configPath)
            dir.eachFileRecurse(FileType.FILES) {
                if (hadoopConfXMLList.contains(it.getName())) {
                    configFiles << it.getAbsolutePath()
                }
            }

            if (service.getProxyuser() != null && service.getProxyuser().principal != "" && service.getProxyuser().keytab != "") {
                return kerberosConnectionObjectProvider.getObject(configFiles, service.getProxyuser().principal, service.getProxyuser().keytab, service.getAuth().principal)
            } else if (!service.getAuth().keytab.isEmpty() && !service.getAuth().principal.isEmpty()) {
                return kerberosConnectionObjectProvider.getObject(configFiles, service.getAuth().principal, service.getAuth().keytab)
            } else if (!service.getAuth().keytab.isEmpty()) {
                return kerberosConnectionObjectProvider.getObject(configFiles, service.getAuth().keytab)
            } else {
                return kerberosConnectionObjectProvider.getObject(configFiles)
            }
        } else {
            throw new ConnectorException("Error create Kerberos connection")
        }
    }

    private static hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    private static List<String> hadoopConfXMLList = ["core-site.xml",
                                                     "hdfs-site.xml",
                                                     "mapred-site.xml",
                                                     "hive-site.xml",
                                                     "yarn-site.xml"]

}