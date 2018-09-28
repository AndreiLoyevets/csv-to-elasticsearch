package com.aloievets.elasticcsv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import com.univocity.parsers.csv.CsvParser;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class CsvToElasticRunner {

    public static void main(String[] args) throws InterruptedException, IOException {
        final String filePath = "testdata.csv";
        final String index = "csvtoelastic-testdata";
        final String type = "doc";

        try (final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class)) {

            final CsvParser parser = context.getBean(CsvParser.class);
            final BulkProcessor bulkProcessor = context.getBean(BulkProcessor.class);

            for (final String[] row : parser.iterate(Files.newInputStream(Paths.get(filePath)))) {
                bulkProcessor.add(new IndexRequest(index, type).source(XContentType.JSON, rowToJsonData(row)));
            }

            bulkProcessor.flush();

            bulkProcessor.awaitClose(5, TimeUnit.SECONDS);
        }
    }

    private static Object[] rowToJsonData(String[] row) {
        return new Object[] {
                "manufacturer", row[0],
                "model", row[1],
                "type", row[2],
                "year", row[3],
                "mileage", row[4],
                "price", row[5]
        };
    }
}
