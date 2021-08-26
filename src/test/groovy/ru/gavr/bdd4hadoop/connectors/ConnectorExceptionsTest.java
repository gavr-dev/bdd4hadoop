package ru.gavr.bdd4hadoop.connectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import ru.gavr.bdd4hadoop.dsl.services.Service;

import java.net.URI;

import static org.junit.Assert.*;

public class ConnectorExceptionsTest {

    @Mock
    private Service service;

    @Test
    public void JDBCConnectionExTest() {
        String cause = null;
        try {
            JDBCConnector connector = new JDBCConnector(service);
            connector.initConnection("http://costom_host");
        } catch (Exception ex) {
            cause = ex.getMessage();
        }
        assertEquals(cause, "Error init connect!");
    }


    @Test
    public void HDFSConnectionExTest() {
        String cause = null;

        try {
            final FileSystem fileSystem = FileSystem.get(URI.create("hdfs://localhost:8020"), new Configuration());
            fileSystem.exists(new Path("/"));
        } catch (Exception e) {
            cause = "Error creation HDFS connect";
        }
        assertEquals(cause, "Error creation HDFS connect");
    }
}