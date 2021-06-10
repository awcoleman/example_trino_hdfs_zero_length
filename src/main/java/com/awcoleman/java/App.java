package com.awcoleman.java;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Simple test file generator to write hourly test files. Not production code.
 * 
 * Does not handle crossing hourly boundaries, assumes intent is to write an hourly parquet file.
 * 
 * Without quick option, generator trickles records into file over very roughly an hour
 * 'Quick' option writes out records without delay
 * 
 * @license Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 * @author awcoleman@gmail.com
 */
public class App {

    private static Schema parseSchema() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            schema = parser.parse(ClassLoader.getSystemResourceAsStream("samplerec.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return schema;
    }

    private static void writeSingleParquet(FileSystem dfs, Schema schema, String datetimein, String pathin,
            boolean quick) {

        int recordsPerFile = 100;
        int milliSleepForHour = 3600000 / recordsPerFile;
        if (! quick) {
            System.out.println("Will sleep for "+milliSleepForHour+" millseconds between writing records.");
        }

        Instant instant = Instant.now();
        int yyyy = instant.atZone(ZoneOffset.UTC).getYear();
        int mm = instant.atZone(ZoneOffset.UTC).getMonthValue();
        int dd = instant.atZone(ZoneOffset.UTC).getDayOfMonth();
        int hh = instant.atZone(ZoneOffset.UTC).getHour();
        int min = instant.atZone(ZoneOffset.UTC).getMinute();
        int ss = instant.atZone(ZoneOffset.UTC).getSecond();
        if (datetimein != null) {
            if (datetimein.length() < 10) {
                System.out.println("Optional argument datetime must be in format YYYYMMDDHH");
                System.exit(1);
            } else {
                try {
                    yyyy = Integer.parseInt(datetimein.substring(0, 4));
                    mm = Integer.parseInt(datetimein.substring(4, 6));
                    dd = Integer.parseInt(datetimein.substring(6, 8));
                    hh = Integer.parseInt(datetimein.substring(8, 10));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }

        String parquetFileRandomName = RandomStringUtils.randomAlphabetic(10);

        String pathprefix = "file:///tmp";
        if (pathin != null) {
            pathprefix = pathin;
        }
        pathprefix = pathprefix + "/year=" + yyyy + "/month=" + mm + "/day=" + dd + "/hour=" + hh;
        Path parquetFile = new Path(pathprefix + "/" + parquetFileRandomName + ".parquet");

        System.out.println("Writing to " + parquetFile);

        ParquetWriter<GenericData.Record> writer = null;
        try {
            writer = AvroParquetWriter.<GenericData.Record>builder(parquetFile).withSchema(schema)
                    .withConf(new Configuration()).withCompressionCodec(CompressionCodecName.SNAPPY).build();

            for (int i = 0; i < recordsPerFile; i++) {
                
                if (! quick) {
                    TimeUnit.MILLISECONDS.sleep(milliSleepForHour);
                    //Thread.sleep(milliSleepForHour);
                }
                instant = Instant.now();
                min = instant.atZone(ZoneOffset.UTC).getMinute();
                ss = instant.atZone(ZoneOffset.UTC).getSecond();

                GenericData.Record record = new GenericData.Record(schema);
                record.put("id", i);
                record.put("name", RandomStringUtils.randomAlphabetic(10));
                record.put("fdatetime",
                        String.format("%04d", yyyy) + String.format("%02d", mm) + String.format("%02d", dd)
                                + String.format("%02d", hh) + String.format("%02d", min) + String.format("%02d", ss));
                writer.write(record);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        Option datetimeOption = new Option("d", "datetime", true, " Datetime in format YYYYMMDDHH");
        Option pathOption = new Option("p", "path", true, " Path prefix, eg file:///tmp or hdfs://data/testfiles");
        Option quickOption = new Option("q", "quick", false, " Quick. Create file and exit");
        Options options = new Options();
        options.addOption(datetimeOption);
        options.addOption(pathOption);
        options.addOption(quickOption);
        CommandLine cli = null;
        String datetimein = null;
        String pathin = null;
        boolean quick = false;
        try {
            cli = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException pe) {
            System.out.println("Error ParseException: " + pe.getMessage());
            System.exit(1);
        }
        if (cli.hasOption(datetimeOption.getOpt())) {
            datetimein = cli.getOptionValue(datetimeOption.getOpt());
        }
        if (cli.hasOption(pathOption.getOpt())) {
            pathin = cli.getOptionValue(pathOption.getOpt());
        }
        if (cli.hasOption(quickOption.getOpt())) {
            quick = true;
        }

        System.out.println("Starting.");
        Schema schema = parseSchema();
        Configuration conf = new Configuration();
        FileSystem dfs = null;
        try {
            dfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        writeSingleParquet(dfs, schema, datetimein, pathin, quick);
        System.out.println("Finished.");
    }
}
