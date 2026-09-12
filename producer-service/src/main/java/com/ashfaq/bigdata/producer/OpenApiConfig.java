package com.ashfaq.bigdata.producer;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Describes the API for Swagger UI, which is the input surface for the live demonstration.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI producerOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Order Producer API")
                .version("1.0.0")
                .description("""
                        Publishes purchase orders to Apache Kafka as Avro records, serialised \
                        through the Confluent Schema Registry.

                        The demonstration endpoints publish the fixed acceptance-case orders:

                        - 1001 / Item1 / 100.00 and 1002 / Item2 / 300.00 process normally
                        - 1003 / TEMP_FAIL / 500.00 fails twice, then succeeds on attempt 3
                        - 1004 / InvalidItem / -10.00 fails permanently and goes to the DLQ
                        - 1005 / ALWAYS_FAIL / 700.00 exhausts its retries, then goes to the DLQ

                        Failed and retried orders never change the running average."""));
    }
}
