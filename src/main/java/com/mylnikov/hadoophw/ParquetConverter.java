package com.mylnikov.hadoophw;

import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Pattern;


public class ParquetConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetConverter.class);

    public static final String CSV_DELIMITER= ",";

    /**
     * Converts csv to parquet and uploads to admin folder.
     */
    public File fromCSV(File srcCSV) throws IOException {
        return fromCSV(srcCSV, "../admin/");
    }

    public File fromCSV(File srcCSV, String destFilder) throws IOException {
        String resultFilePath = destFilder + srcCSV.getName().split("\\.")[0]+".parquet";
        LOGGER.info("Result file name " + resultFilePath);
        Path path = new Path(resultFilePath);
        String rawSchema = getSchema(srcCSV);
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        try( CsvParquetWriter writer = new CsvParquetWriter(path, schema, false)) {
            String line;
            try (BufferedReader br = new BufferedReader(new FileReader(srcCSV))) {
                br.readLine();
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                    writer.write(Arrays.asList(fields));
                }
            }
        }
        return new File(resultFilePath);
    }

    private String getSchema(File csvFile) throws IOException {
        String firstLine = Files.readFirstLine(csvFile, Charset.defaultCharset());
        return buildProtobufSchemaMessageSchema(firstLine.split(","));
    }

    private String buildProtobufSchemaMessageSchema(String[] fields) {
        StringBuilder resultMessage = new StringBuilder().append("message csv {");
        int fieldCount = 1;
        for (String field: fields) {
            resultMessage.append("required binary "+field + " = " + fieldCount++ + ";");
        }
        resultMessage.append(" }");
        return resultMessage.toString();
    }

}
