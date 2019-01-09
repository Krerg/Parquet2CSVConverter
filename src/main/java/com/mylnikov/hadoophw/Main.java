package com.mylnikov.hadoophw;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        ParquetConverter parquetConverter = new ParquetConverter();
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path("hdfs://test.com:9000/user/test/in"));  // you need to pass in your hdfs path

        for (FileStatus file : status) {
            File csv = new File(String.valueOf(IOUtils.toByteArray(fs.open(file.getPath()))));
            parquetConverter.fromCSV(csv);
        }

        fs.close();
    }

}
