package com.mylnikov.hadoophw;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Controller for operating hdfs.
 */
public class HdfsController {

    private final Configuration CONFIG;

    private final FileSystem FS;

    public HdfsController(String baseuri) throws IOException {
        CONFIG = new Configuration();
        CONFIG.set("fs.defaultFS", baseuri);
        CONFIG.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        CONFIG.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        FS = FileSystem.get(CONFIG);
    }

    /**
     * Get files metadata from hdfs folder.
     */
    public FileStatus[] getFiles(String folder) throws IOException {
        return FS.listStatus(new Path(folder));
    }

    /**
     * Gets stream for downloading file.
     */
    public InputStream getInputStream(Path path) throws IOException {
        return FS.open(path);
    }

    public void uploadFile(File file, String folder) throws IOException {
        Path uploadPath = new Path(folder + "/" + file.getName());
        System.out.println("Uploading to:" + uploadPath.toString());
        try(FSDataOutputStream outputStream = FS.create(uploadPath); FSDataInputStream inputStream = FS.open(new Path("/user/root/" + file.getName()))) {
            IOUtils.copy(inputStream, outputStream);
        }
    }

}
