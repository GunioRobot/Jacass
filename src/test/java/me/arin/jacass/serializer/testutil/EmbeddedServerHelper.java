package me.arin.jacass.serializer.testutil;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;

import java.io.*;

/**
 * Jacked from Hector: http://github.com/rantav/hector
 */

public class EmbeddedServerHelper {

    private static final String TMP = "tmp";
    private EmbeddedCassandraService cassandra;
    private String baseDir;
    private static String testResourceDir;

    public EmbeddedServerHelper(String baseDir) {
        this.baseDir = baseDir;
        testResourceDir = this.baseDir + "/src/test/resources";
    }

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws org.apache.thrift.transport.TTransportException
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    public void setup() throws TTransportException, IOException, InterruptedException {
        // delete tmp dir first
        rmdir(TMP);
        // make a tmp dir and copy storag-conf.xml and log4j.properties to it
        copy("/storage-conf.xml", TMP);
        copy("/log4j.properties", TMP);
        System.setProperty("storage-config", TMP);

        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        cassandra = new EmbeddedCassandraService();
        cassandra.init();
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
    }

    public void teardown() throws IOException {
        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.cleanupDataDirectories();
        rmdir(TMP);
    }

    private static void rmdir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists()) {
            FileUtils.deleteDir(new File(dir));
        }
    }

    /**
     * Copies a resource from within the jar to a directory.
     *
     * @param resource
     * @param directory
     * @throws IOException
     */
    private static void copy(String resource, String directory) throws IOException {
        mkdir(directory);
        InputStream is = new FileInputStream(testResourceDir + resource);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + System.getProperty("file.separator") + fileName);
        OutputStream out = new FileOutputStream(file);
        byte buf[] = new byte[1024];
        int len;
        while ((len = is.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.close();
        is.close();
    }

    /**
     * Creates a directory
     *
     * @param dir
     * @throws IOException
     */
    private static void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }

}
