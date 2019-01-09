package com.mylnikov.hadoophw;

import org.apache.hadoop.fs.Path;

import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;


public class ParquetConverter {

    public static final String CSV_DELIMITER= "|";

    public File fromCSV(File srcCSV) throws IOException {
        Path path = new Path(srcCSV.getName()+"prq");
        String rawSchema = getSchema(srcCSV);

        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, false);


        String line;
        int lineNumber = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(srcCSV));) {
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                writer.write(Arrays.asList(fields));
                ++lineNumber;
            }
            writer.close();
        }
        return null;

    }

    private String readFile(String path) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(path));) {
            String line;
            String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }

            return stringBuilder.toString();
        }
    }

    public String getSchema(File csvFile) throws IOException {
        String fileName = csvFile.getName().substring(
                0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        return readFile(schemaFile.getAbsolutePath());
    }

}
