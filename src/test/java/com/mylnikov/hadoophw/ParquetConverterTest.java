package com.mylnikov.hadoophw;

import org.junit.Assert;
import org.junit.Test;

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
        resultFile.delete();
        parquetCrcFile.delete();
    }

}
