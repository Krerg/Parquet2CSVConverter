package com.mylnikov.hadoophw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;

import java.io.File;
import java.io.IOException;

public class ParquetConverterTest {

    private ParquetConverter parquetConverter = new ParquetConverter();

    @Test
    public void shouldConvertCsvToParquet() throws IOException {
        File resultFile = parquetConverter.fromCSV(new File("src/test/resources/sample_submission.csv"), "src/test/resources/");
        Assert.assertEquals(resultFile.exists(), true);
        File parquetCrcFile = new File("src/test/resources/.sample_submission.parquet.crc");
        Assert.assertEquals(parquetCrcFile.exists(), true);
        resultFile.deleteOnExit();
        parquetCrcFile.deleteOnExit();
        Configuration conf = new Configuration();
        ParquetReader reader =
                ParquetReader.builder(new GroupReadSupport(), new Path(resultFile.getAbsolutePath()))
                        .withConf(conf)
                        .build();
        SimpleGroup data = (SimpleGroup)reader.read();
        Assert.assertEquals(data.getBinary(0,0).toStringUsingUTF8(), "0");
        Assert.assertEquals(data.getBinary(1,0).toStringUsingUTF8(), "99 1");

    }

}
