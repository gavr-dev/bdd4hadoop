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

    KerberosConnection(String principal, String keytabFile, String metaStoreUris) {
        Configuration conf = new Configuration(true)

        for (String xml : hadoopConfXMLList) {
            conf.addResource(new HadoopPath(getRealFile(hadoopConfDir+File.separator+xml).toString()) )
        }

        keytabFile = keytabFile?:"/etc/hive.keytab"
        principal = principal?:"hive/_HOST@REALM"

        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUris)
        conf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true")
        conf.set("hive.metastore.kerberos.keytab.file", keytabFile)
        conf.set("hive.server2.authentication.kerberos.keytab", keytabFile)
        conf.set("hive.server2.authentication.kerberos.principal", principal)
        conf.set("hive.metastore.kerberos.principal", principal)
        conf.set("hadoop.security.authentication", "KERBEROS")
        conf.set("hive.metastore.authentication", "KERBEROS")

        UserGroupInformation.setConfiguration(conf)
        def ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keytabFile)
        setUgi(ugi)
        ugi.setLoginUser(ugi)
        log.info("${ugi.getLoginUser()} ${ugi.getCurrentUser()} ${ugi.isSecurityEnabled()} ${ugi.isLoginKeytabBased()} ${ugi.isLoginTicketBased()}")
        setConf(conf)
    }

    KerberosConnection(String proxyUserPrincipal,
                       String proxyUserKeytabFile,
                       String userPrincipal,
                       String metaStoreUris) {
        Configuration conf = new Configuration(true)

        for (String xml : hadoopConfXMLList) {
            conf.addResource(new HadoopPath(getRealFile(hadoopConfDir+File.separator+xml).toString()) )
        }

        proxyUserKeytabFile = proxyUserKeytabFile?:"/etc/hive.keytab"
        proxyUserPrincipal = proxyUserPrincipal?:"hive/_HOST@REALM"

        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUris)
        conf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true")
        conf.set("hive.metastore.kerberos.keytab.file", proxyUserKeytabFile)
        conf.set("hive.server2.authentication.kerberos.keytab", proxyUserKeytabFile)
        conf.set("hive.server2.authentication.kerberos.principal", proxyUserPrincipal)
        conf.set("hive.metastore.kerberos.principal", proxyUserPrincipal)
        conf.set("hadoop.security.authentication", "KERBEROS")
        conf.set("hive.metastore.authentication", "KERBEROS")
//        conf.set(HiveConf.ConfVars.HIVE_DDL_OUTPUT_FORMAT.varname, "json")

        UserGroupInformation.setConfiguration(conf)
        def proxyUserUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(proxyUserPrincipal, "0.0.0.0"), proxyUserKeytabFile)
        def userUgi = UserGroupInformation.createProxyUser(userPrincipal, proxyUserUgi)

        setUgi(userUgi)
        ugi.setLoginUser(userUgi)
        log.info("${userUgi.getLoginUser()} ${userUgi.getCurrentUser()} ${userUgi.isSecurityEnabled()} ${userUgi.isLoginKeytabBased()} ${userUgi.isLoginTicketBased()}")
        setConf(conf)
    }



    KerberosConnection(boolean fal) {
        Configuration conf = new Configuration(false)

        for (String xml : hadoopConfXMLList) {
            conf.addResource(new HadoopPath(getRealFile(hadoopConfDir+File.separator+xml).toString()))
        }

        conf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true")
        conf.set("hadoop.security.authentication", "simple")
        conf.set("hive.metastore.authentication", "PLAIN")

        UserGroupInformation.setConfiguration(conf)
        def ugi = UserGroupInformation.getLoginUser()
        log.info("${ugi.getLoginUser()} ${ugi.getCurrentUser()} ${ugi.isSecurityEnabled()} ${ugi.isLoginKeytabBased()} ${ugi.isLoginTicketBased()}")
        setUgi(ugi)
        setConf(conf)
    }


    //accept config list paths for hdfs
    KerberosConnection(List<String> configFiles, boolean loginFromKeytab) {
        log.debug("File paths: " + configFiles )
        Configuration conf = new Configuration(false)

        configFiles.each {
            conf.addResource(new HadoopPath(getRealFile(it).toString()))
        }

        UserGroupInformation.setConfiguration(conf)
        def ugi = UserGroupInformation.getLoginUser()
        setUgi(ugi)
        log.debug("Intialized configuration for HDFS Service. Login user: ${ugi.getLoginUser()}, current user: ${ugi.getCurrentUser()}")
        setConf(conf)
    }

    KerberosConnection(List<String> configFiles, String keytabPath) {
        log.debug("File paths: " + configFiles )
        Configuration conf = new Configuration(false)

        configFiles.each {
            conf.addResource(new HadoopPath(getRealFile(it).toString()))
        }

        UserGroupInformation.setConfiguration(conf)
        def ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                SecurityUtil.getServerPrincipal(conf.get("dfs.namenode.kerberos.principal"), "0.0.0.0"),
                keytabPath
        )
        setUgi(ugi)
        ugi.setLoginUser(ugi)
        log.info("Intialized configuration for HDFS Service. Login user: ${ugi.getLoginUser()}, current user: ${ugi.getCurrentUser()}")
        setConf(conf)
    }

    KerberosConnection(List<String> configFiles, String principal, String keytabPath) {
        log.debug("File paths: " + configFiles )
        Configuration conf = new Configuration(false)

        configFiles.each {
            conf.addResource(new HadoopPath(getRealFile(it).toString()))
        }

        UserGroupInformation.setConfiguration(conf)
        def ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                SecurityUtil.getServerPrincipal(principal, "0.0.0.0"),
                keytabPath
        )
        setUgi(ugi)
        ugi.setLoginUser(ugi)
        log.info("Intialized configuration for HDFS Service. Login user: ${ugi.getLoginUser()}, current user: ${ugi.getCurrentUser()}")
        setConf(conf)
    }

    KerberosConnection(List<String> configFiles, String proxyUserPrincipal, String proxyUserKeytabPath, String userPrincipal) {
        log.debug("File paths: " + configFiles )
        Configuration conf = new Configuration(false)

        configFiles.each {
            conf.addResource(new HadoopPath(getRealFile(it).toString()))
        }

        UserGroupInformation.setConfiguration(conf)
        def proxyUserUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                SecurityUtil.getServerPrincipal(proxyUserPrincipal, "0.0.0.0"),
                proxyUserKeytabPath
        )
        def userViaProxyUgi = UserGroupInformation.createProxyUser(userPrincipal, proxyUserUgi)
        setUgi(userViaProxyUgi)
        ugi.setLoginUser(userViaProxyUgi)
        log.info("Intialized configuration for HDFS Service. Login user: ${userViaProxyUgi.toString()}")
        setConf(conf)
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