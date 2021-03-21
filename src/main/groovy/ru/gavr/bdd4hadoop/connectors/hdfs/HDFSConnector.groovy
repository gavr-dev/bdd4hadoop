package ru.gavr.bdd4hadoop.connectors.hdfs

import com.google.common.io.CharStreams
import com.jcraft.jsch.ChannelExec
import com.jcraft.jsch.JSch
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.Session
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.security.UserGroupInformation
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.connectors.RunAutotestCommandConnector
import ru.gavr.bdd4hadoop.connectors.utils.KerberosConnection
import ru.gavr.bdd4hadoop.dsl.services.Service
import ru.gavr.bdd4hadoop.enums.HDFSCommandType
import ru.gavr.bdd4hadoop.exceptions.NoSuchCommandException

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.security.PrivilegedExceptionAction

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class HDFSConnector implements RunAutotestCommandConnector{

    private Service service
    private UserGroupInformation clientUgi
    private FileSystem fs

    private KerberosConnection kerberosConnection

    @Autowired
    ObjectProvider<KerberosConnection> kerberosConnectionObjectProvider

    HDFSConnector(Service service){
        this.service = service
    }

    @PostConstruct
    void construct(){
        log.info("Started to initialize HDFS connector. Config path: " + service.getAuth().configPath)
        kerberosConnection = KerberosConnection.createForService(service, kerberosConnectionObjectProvider)
        log.debug("Kerberos connection are enable")

        def conf = kerberosConnection.getConf()
        if (service.getConfig().getBlockSize() > 0) {
            conf.setLong("dfs.block.size", service.getConfig().getBlockSize())
        }
        fs = FileSystem.get(conf)
        clientUgi = kerberosConnection.getUgi()
        log.info("HDFS connector initialized for " + clientUgi.getUserName())
    }

    def runQuery(String query){
        def result = ["FAILURE", "SUCCESS"]
        String[] params = query.split(" ")
        String commandType = params[0].replace(":", "")
        String paramValue = params[1]
        log.info("Execute \"${query}\"")

        clientUgi.doAs(new PrivilegedExceptionAction<Void>() {

            @Override
            Void run() throws Exception {
                def commands = []
                HDFSCommandType.values().each {
                    commands << it.getCommandValue()
                }
                if(!commands.contains(commandType)) {
                    throw new NoSuchCommandException("${commandType}")
                }
                try{
                    switch (commandType) {
                        case HDFSCommandType.TOUCHZ.getCommandValue():
                            fs.create(new HadoopPath(paramValue))
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.CREATE_DIRECTORY.getCommandValue():
                            if (fs.exists(new HadoopPath(paramValue))) {
                                result = [commandType, "SUCCESS"]
                                break;
                            }
                            fs.mkdirs(new HadoopPath(paramValue))
                            if (params.length > 2) {
                                fs.setPermission(new HadoopPath(paramValue), new FsPermission(params[2]))
                            }
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.DELETE.getCommandValue():
                            fs.delete(new HadoopPath(paramValue), true)
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.DOWNLOAD.getCommandValue():
                            String destPath = (params.length > 2) ? params[2] : System.getProperty("user.dir") + System.getProperty("file.separator") + paramValue
                            fs.copyToLocalFile(new HadoopPath(paramValue), new HadoopPath(destPath))
                            result = [commandType, "SUCCESS"]
                            break

                    }
                    return
                } catch(RemoteException re) {
                    result = ["FAILURE", re.getMessage().tokenize("\n")[0]]
                    return
                } catch(Exception e) {
                    result = ["FAILURE", e.toString()]
                    return
                }
            }

        })

        if (result.get(0).equals("FAILURE")) {
            log.info("Result executing: failure")
        } else {
            log.info("Result executing: success")
        }
        return result
    }



    static def closeConnection(ChannelExec channel, Session session) {
        try {
            channel.disconnect()
            session.disconnect()
        } catch (Exception ignored) {
        }
    }

    static def getUploadPath(String from, String to){
        def split = from.split(System.getProperty("file.separator"))
        return to + System.getProperty("file.separator") + split[split.length -1]
    }

    @PreDestroy
    void destroy(){

        log.info("Started to turn off the HDFS connector")
        fs.close()
        log.info("Turned off the HDFS connector")

    }

}
