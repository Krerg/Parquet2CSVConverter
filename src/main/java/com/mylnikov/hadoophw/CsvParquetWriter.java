package com.mylnikov.hadoophw;

import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

/**
 * Wrapper for {@link ParquetWriter} for easily bypassing default parameters (like {@link CompressionCodecName#UNCOMPRESSED} )
 */
public class CsvParquetWriter extends ParquetWriter<List<String>> {

    public CsvParquetWriter(Path file, MessageType schema, boolean enableDictionary) throws IOException {
        this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary);
    }

    public CsvParquetWriter(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary) throws IOException {
        super(file, new CsvWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
    }
}