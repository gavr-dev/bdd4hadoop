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
                        case HDFSCommandType.UPLOAD.getCommandValue():
                            String flag = params[1].startsWith("-") ? params[1] : ""
                            params = params - flag
                            if (fs.isDirectory(new HadoopPath(params[2]))) {
                                params[2] = params[2] + "/" + params[1].split("/").last()
                            }
                            HadoopPath fromPath = new HadoopPath(params[1])
                            HadoopPath toPath = new HadoopPath(params[2])
                            HadoopPath tempFile = new HadoopPath(params[2] + "._COPYING_")
                            if (flag == "-f") {
                                fs.delete(toPath, true)
                            } else if (fs.exists(toPath)) {
                                result = ["FAILURE", params[2] + " file exists"]
                                break
                            }
                            try {
                                fs.copyFromLocalFile(false, true, fromPath, tempFile)
                            } catch (IOException e) {
                                fs.delete(tempFile, true)
                                throw e;
                            }
                            fs.rename(tempFile, toPath)
                            if (params.length > 3) {
                                fs.setPermission(toPath, new FsPermission(params[3]))
                            }
                            if (flag == "-p") {
                                def filesStatus = fs.getLocal(fs.getConf()).getFileStatus(fromPath)
                                fs.setPermission(toPath, filesStatus.getPermission())
                                fs.setOwner(toPath, filesStatus.getOwner(), filesStatus.getGroup())
                                fs.setTimes(toPath, filesStatus.getModificationTime(), filesStatus.getAccessTime())
                            } else if (flag == "-l") {
                                fs.setReplication(toPath, 1 as short)
                            }
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.LIST.getCommandValue():
                            HadoopPath newPath = new HadoopPath(paramValue)
                            FileStatus[] ls = fs.listStatus(newPath, newPath)
                            result = [ls.join("\n"), "SUCCESS"]
                            break
                        case HDFSCommandType.CHMOD.getCommandValue():
                            fs.setPermission(new HadoopPath(params[1]), new FsPermission(params[2]))
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.CHOWN.getCommandValue():
                            fs.setOwner(new HadoopPath(params[1]), params[2], params[3])
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.READ.getCommandValue():
                            List<Map<String, Object>> processedData =
                                    HDFSParquetReader.readData(paramValue)
                            result = [processedData, "SUCCESS"]
                            break
                        case HDFSCommandType.COUNT_PARQUET_ROWS.getCommandValue():
                            int processedData =
                                    HDFSParquetReader.countDataRows(paramValue)
                            result = [processedData, "SUCCESS"]
                            break
                        case HDFSCommandType.READ_FILE_LINES.getCommandValue():
                            def lines = Files.readAllLines(Paths.get(paramValue))
                            result = [lines, "SUCCESS"]
                            break
                        case HDFSCommandType.CHANGE_PARAM.getCommandValue():
                            String res = change_param(params)
                            result = res == params[4] ? [res, "SUCCESS"] : ["FAILURE", res]
                            break
                        case HDFSCommandType.SLEEP.getCommandValue():
                            sleep(1000 * Integer.parseInt(paramValue))
                            result = [query, "SUCCESS"]
                            break
                        case HDFSCommandType.MOVE.getCommandValue():
                            fs.rename(new HadoopPath(params[1]), new HadoopPath(params[2]));
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.COPY.getCommandValue():
                            FileUtil.copy(fs, new HadoopPath(params[1]), fs, new HadoopPath(params[2]), false, new Configuration());
                            result = [commandType, "SUCCESS"]
                            break
                        case HDFSCommandType.DIFF.getCommandValue():
                            Process process = Runtime.getRuntime().exec(String.format("diff %s %s", params[1], params[2]));
                            String resultCmd = process.getText();
                            if (resultCmd.isEmpty()) {
                                result = [commandType, "SUCCESS"]
                            } else {
                                result.set(1, resultCmd);
                            }
                            break
                        case HDFSCommandType.CREATE_FILE.getCommandValue():
                            def createParams = query.split(" ", 3);
                            if (createParams.size() != 3) {
                                result = ["FAILURE", "Create command should be: command path 'Text to file.'"]
                                break
                            }
                            HadoopPath filePath = new HadoopPath(createParams[1])
                            def outputStream;
                            try {
                                fs.createNewFile(filePath)
                                outputStream = fs.append(filePath)
                                outputStream.write(createParams[2].getBytes(StandardCharsets.UTF_8))
                                result = [commandType, "SUCCESS"]
                            } catch(Exception ex) {
                                result = ["FAILURE", ex.getMessage()]
                            } finally {
                                if (outputStream != null) {
                                    outputStream.close()
                                }
                            }
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

    static def change_param(String[] params){
        String host = params[1]
        String filename = params[2]
        String parameter = params[3]
        String value = params[4]
        String prvkey = params.length == 6 ? params[5] : "/root/.ssh/id_rsa"

        JSch jsch = new JSch()
        Session session = null
        ChannelExec channel = null
        try {
            jsch.addIdentity(prvkey)
            session = jsch.getSession(host)
            session.setConfig("PreferredAuthentications", "publickey")
            session.setConfig("StrictHostKeyChecking", "no")
            session.connect()
            channel = (ChannelExec) session.openChannel("exec")
            String command = "yq eval -i '" + parameter + " = \"" + value + "\"' " + filename +
                    " && yq eval \"" + parameter + "\" " + filename
            channel.setCommand(command)
            channel.setInputStream(null)
            InputStream output = channel.getInputStream()
            channel.connect()
            String result = CharStreams.toString(new InputStreamReader(output)).takeAfter("\n").trim()
            return result

        } catch (JSchException | IOException e) {
            closeConnection(channel, session)
            throw new RuntimeException(e)

        } finally {
            closeConnection(channel, session)
        }
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
