package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        // Initialize the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3)); // Retry strategy

        // Define the Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") // Kafka broker address
                .setGroupId("flink-group")            // Consumer group ID
                .setTopics("whatever.public.following") // Kafka topic
                .setStartingOffsets(OffsetsInitializer.latest()) // Start from earliest offset
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize as String
                .build();

        // Create a DataStream from the Kafka source
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Parse JSON, extract fields, and log them
        DataStream<Tuple2<String, String>> userFollowerStream = kafkaStream.map(json -> {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(json);

            // Extract "user" and "follower" from the "after" field
            String user = rootNode.path("payload").path("after").path("user").asText();
            String follower = rootNode.path("payload").path("after").path("follower").asText();

            // Log the values
            System.out.println("User: " + user + ", Follower: " + follower);

            return Tuple2.of(user, follower);
        }).returns(TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, String>>() {}));

        // Define the JDBC sink for PostgreSQL
        userFollowerStream.addSink(JdbcSink.sink(
                "INSERT INTO followers (\"user\", follower) VALUES (?, ?)", // Corrected SQL query
                (statement, tuple) -> { // Statement preprocessor
                    statement.setString(1, tuple.f1); // Set "user"
                    statement.setString(2, tuple.f0); // Set "follower"
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10) // Batch size
                        .withBatchIntervalMs(200) // Batch interval
                        .withMaxRetries(2) // Retries
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/testdb") // PostgreSQL URL
                        .withDriverName("org.postgresql.Driver") // Driver class
                        .withUsername("postgres") // Database username
                        .withPassword("password") // Database password
                        .build()
        ));

        // Execute the Flink job
        env.execute("Kafka to Flink to PostgreSQL Example");
    }
}
