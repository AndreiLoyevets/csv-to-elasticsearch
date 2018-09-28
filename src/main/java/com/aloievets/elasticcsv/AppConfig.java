package com.aloievets.elasticcsv;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.function.BiConsumer;

@Configuration
@ComponentScan
public class AppConfig {

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @Bean
    public BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer(RestHighLevelClient client) {
        return (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
    }

    @Bean
    public BulkProcessor.Listener bulkListener() {
        return new Listener() {
            @Override public void beforeBulk(final long l, final BulkRequest bulkRequest) {
            }

            @Override public void afterBulk(final long l, final BulkRequest bulkRequest,
                    final BulkResponse bulkResponse) {
            }

            @Override public void afterBulk(final long l, final BulkRequest bulkRequest, final Throwable throwable) {
            }
        };
    }

    @Bean
    public BulkProcessor bulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer,
                                          BulkProcessor.Listener bulkListener) {
        return BulkProcessor.builder(bulkConsumer, bulkListener).setConcurrentRequests(10).build();
    }

    @Bean
    public CsvParserSettings csvParserSettings() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(';');

        return settings;
    }

    @Bean
    public CsvParser csvParser(CsvParserSettings settings) {
        return new CsvParser(settings);
    }
}
