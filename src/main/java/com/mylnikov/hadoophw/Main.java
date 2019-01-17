package com.mylnikov.hadoophw;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_HDFS_URI = "hdfs://sandbox-hdp.hortonworks.com:8020";

    public static void main(String[] args) throws IOException {
        HdfsController hdfsController = new HdfsController(args.length > 0 ? args[0] : DEFAULT_HDFS_URI);
        LOGGER.info("HDFSController successfull initialized");
        try {
            convertFilesToParquetFromFolder(hdfsController, "/usr/admin");
        } catch (IOException ex) {
            LOGGER.error("Unable to parse files: " + ex.getLocalizedMessage());
            System.exit(1);
        }
    }

    private static void convertFilesToParquetFromFolder(HdfsController hdfsController, String folder) throws IOException {
        ParquetConverter parquetConverter = new ParquetConverter();
        FileStatus[] status = hdfsController.getFiles(folder);
        if (status == null || status.length == 0) {
            return;
        }
        for (FileStatus file : status) {
            if(file.isDirectory()) {
                continue;
            }
            File out = new File(file.getPath().getName());
            try {
                FileUtils.copyToFile(hdfsController.getInputStream(file.getPath()), out);
                LOGGER.info("File: {} was successfully downloaded", out.getName());
                parquetConverter.fromCSV(out);
                LOGGER.info("File: {}  was successfully converted to parquet and uploaded to admin folder", out.getName());
            }  finally {
                out.delete();
            }

        }
    }

}
