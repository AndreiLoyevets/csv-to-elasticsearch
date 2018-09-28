package com.aloievets.elasticcsv;

import java.util.function.BiConsumer;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

@Configuration
@ComponentScan
public class AppConfig {

    @Bean
    public CredentialsProvider credentialsProvider() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("username", "password"));

        return credentialsProvider;
    }

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticClient(CredentialsProvider credentialsProvider) {

        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("host", 9200, "https"))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)));
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
