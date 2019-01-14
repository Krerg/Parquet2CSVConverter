package com.mylnikov.hadoophw;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Main {

    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        HdfsController hdfsController = new HdfsController("hdfs://sandbox-hdp.hortonworks.com:8020");
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
                LOGGER.info("File:" + out.getName() + " was successfully downloaded");
                parquetConverter.fromCSV(out);
                LOGGER.info("File:" + out.getName() + " was successfully converted to parquet and uploaded to admin folder");
            }  finally {
                out.delete();
            }

        }
    }

}
