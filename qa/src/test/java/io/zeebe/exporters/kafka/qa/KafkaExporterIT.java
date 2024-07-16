/*
 * Copyright © 2019 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporters.kafka.qa;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.exporter.test.ExporterTestConfiguration;
import io.camunda.zeebe.exporter.test.ExporterTestContext;
import io.camunda.zeebe.exporter.test.ExporterTestController;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.test.broker.protocol.ProtocolFactory;
import io.zeebe.exporters.kafka.KafkaExporter;
import io.zeebe.exporters.kafka.config.Config;
import io.zeebe.exporters.kafka.config.ProducerConfig;
import io.zeebe.exporters.kafka.config.RecordConfig;
import io.zeebe.exporters.kafka.config.RecordsConfig;
import io.zeebe.exporters.kafka.serde.RecordDeserializer;
import io.zeebe.exporters.kafka.serde.RecordId;
import io.zeebe.exporters.kafka.serde.RecordIdDeserializer;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.agrona.CloseHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * This tests the deployment of the exporter into a Zeebe broker in a as-close-to-production way as
 * possible, by starting a Zeebe container and deploying the exporter as one normally would.
 *
 * <p>In order to verify certain properties - i.e. all records were exported correctly, order was
 * maintained on a per partition basis, etc. - we use an exporter deemed "reliable", the
 * DebugHttpExporter, to compare results.
 */
@Testcontainers
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@Execution(ExecutionMode.CONCURRENT)
final class KafkaExporterIT {
  private static final Pattern TOPIC_SUBSCRIPTION_PATTERN = Pattern.compile("zeebe.*");

  private final Network network = Network.newNetwork();
  private KafkaContainer kafkaContainer = newKafkaContainer();

  private ZeebeClient zeebeClient;
  private DebugHttpExporterClient debugExporter;

  private final ExporterTestController controller = new ExporterTestController();
  private final ProtocolFactory factory = new ProtocolFactory();
  private static final RecordConfig DEFAULT_RECORD_CONFIG =
      new RecordConfig(EnumSet.allOf(RecordType.class), "zeebe");

  private Config config;
  private KafkaExporter exporter;
  private ExporterTestContext exporterTestContext;

  @BeforeAll
  public void beforeAll() {
    config =       new Config(
          new ProducerConfig("client", Duration.ofSeconds(1), new HashMap<>(), Duration.ofSeconds(1), Duration.ofSeconds(1), List.of(kafkaContainer.getBootstrapServers()), "prefix"),
          new RecordsConfig(new HashMap<>(), DEFAULT_RECORD_CONFIG),
          100,
          Duration.ofSeconds(1)
          );
    exporterTestContext =
        new ExporterTestContext()
            .setConfiguration(new ExporterTestConfiguration<>("kafka", config));
    exporter.configure(exporterTestContext);
    exporter.open(controller);
  }

  @AfterEach
  void tearDown() {
    CloseHelper.quietCloseAll(zeebeClient, kafkaContainer, network);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("io.camunda.zeebe.exporter.TestSupport#provideValueTypes")
  void shouldExportRecord(final ValueType valueType) {
    // given
    final var record = factory.generateRecord(valueType);

    // when
    export(record);

    // then
    final var response = testClient.getExportedDocumentFor(record);
    assertThat(response)
        .extracting(GetResponse::index, GetResponse::id, GetResponse::routing, GetResponse::source)
        .containsExactly(
            indexRouter.indexFor(record),
            indexRouter.idFor(record),
            String.valueOf(record.getPartitionId()),
            record);
  }

  @Test
  void shouldExportEvenIfKafkaStartedLater() throws MalformedURLException {
    // given
    exporterTestContext =
        new ExporterTestContext()
            .setConfiguration(new ExporterTestConfiguration<>("kafka", config));
    exporter.configure(exporterTestContext);
    exporter.open(controller);
    final var sampleWorkload = newSampleWorkload();

    // when
    sampleWorkload.execute();
    startKafka();

    // then
    assertRecordsExported(sampleWorkload);
  }

  @Test
  void shouldExportEvenIfKafkaRestartedInTheMiddle()
      throws MalformedURLException, InterruptedException {
    // given
    startKafka();
    exporterTestContext =
        new ExporterTestContext()
            .setConfiguration(new ExporterTestConfiguration<>("kafka", config));
    exporter.configure(exporterTestContext);
    exporter.open(controller);
    final var sampleWorkload = newSampleWorkload();

    // when
    final var latch = new CountDownLatch(1);
    final var workloadFinished =
        CompletableFuture.runAsync(() -> sampleWorkload.execute(latch::countDown));

    assertThat(latch.await(15, TimeUnit.SECONDS))
        .as("midpoint hook was called to stop kafka")
        .isTrue();
    kafkaContainer.stop();
    kafkaContainer = newKafkaContainer();
    startKafka();
    workloadFinished.join();

    // then
    assertRecordsExported(sampleWorkload);
  }

  private boolean export(final Record<?> record) {
    exporter.export(record);
    return true;
  }

  /**
   * Asserts that the expected records have been correctly exported.
   *
   * <p>The properties asserted are the following for every partition:
   *
   * <ol>
   *   <li>every record for partition X was exported to Kafka
   *   <li>every record for partition X was exported to the same Kafka partition Y
   *   <li>every record for partition X is consumed in the order in which they were written (i.e. by
   *       position)
   * </ol>
   *
   * The first property is self explanatory - just ensure all records can be consumed from the
   * expected Kafka topic.
   *
   * <p>The second property checks the partitioning logic - Zeebe records are causally linked, and
   * exporting them to different partitions will result in them being consumed out of order. So this
   * ensures that all records from a given Zeebe partition are exported to the same Kafka partition
   * in order to preserve ordering.
   *
   * <p>The third property is an extension of this, and checks that they are indeed ordered by
   * position.
   */
  private void assertRecordsExported(final SampleWorkload workload) {
    final var expectedRecords = workload.getExpectedRecords(Duration.ofSeconds(5));
    final var expectedRecordsPerPartition =
        expectedRecords.stream().collect(Collectors.groupingBy(Record::getPartitionId));
    final var actualRecords = awaitAllExportedRecords(expectedRecordsPerPartition);

    assertThat(expectedRecords).as("there should have been some records exported").isNotEmpty();
    assertThat(actualRecords)
        .allSatisfy(
            (partitionId, records) -> {
              assertExportedRecordsPerPartition(
                  partitionId, records, expectedRecordsPerPartition.get(partitionId));
            });
  }

  @SuppressWarnings("rawtypes")
  private void assertExportedRecordsPerPartition(
      final Integer partitionId,
      final List<ConsumerRecord<RecordId, Record<?>>> exportedRecords,
      final List<Record<?>> expectedRecords) {
    final var expectedKafkaPartition = exportedRecords.get(0).partition();
    assertThat(exportedRecords)
        .as(
            "all exported records from Zeebe partition %d were exported to the same Kafka partition %d",
            partitionId, expectedKafkaPartition)
        .allMatch(r -> r.partition() == expectedKafkaPartition)
        // cast to raw type to be able to compare the containers
        .map(r -> (Record) r.value())
        .as(
            "the records for partition %d are the same as those reported by the DebugHttpExporter",
            partitionId)
        .containsExactlyInAnyOrderElementsOf(expectedRecords)
        .as("the records for partition %d are sorted by position", partitionId)
        .isSortedAccordingTo(Comparator.comparing(Record::getPosition));
  }

  /**
   * A wrapper around {@link #consumeExportedRecords(Map)} to avoid race conditions where we poll
   * too early and receive less records. Doing this avoids any potential flakiness at the cost of a
   * bit more complexity/unreadability.
   */
  private Map<Integer, List<ConsumerRecord<RecordId, Record<?>>>> awaitAllExportedRecords(
      final Map<Integer, List<Record<?>>> expectedRecords) {
    final var records = new HashMap<Integer, List<ConsumerRecord<RecordId, Record<?>>>>();

    Awaitility.await("until the expected number of records has been consumed")
        .atMost(Duration.ofSeconds(30))
        .pollDelay(Duration.ZERO)
        .pollInterval(Duration.ofMillis(100))
        .pollInSameThread()
        .untilAsserted(
            () -> {
              consumeExportedRecords(records);
              assertThat(records)
                  .allSatisfy(
                      (partitionId, list) -> {
                        assertThat(list)
                            .as("records consumed for partition %d", partitionId)
                            .hasSameSizeAs(expectedRecords.get(partitionId));
                      });
            });

    return records;
  }

  private void consumeExportedRecords(
      final Map<Integer, List<ConsumerRecord<RecordId, Record<?>>>> records) {
    final var timeout = Duration.ofSeconds(5);

    try (final Consumer<RecordId, Record<?>> consumer = newConsumer()) {
      final var consumedRecords = consumer.poll(timeout);
      for (final var consumedRecord : consumedRecords) {
        final var perPartitionRecords =
            records.computeIfAbsent(
                consumedRecord.value().getPartitionId(), ignored -> new ArrayList<>());

        perPartitionRecords.add(consumedRecord);
        perPartitionRecords.sort(Comparator.comparing(ConsumerRecord::offset, Long::compareTo));
      }
    }
  }

  private Consumer<RecordId, Record<?>> newConsumer() {
    final var config = new HashMap<String, Object>();
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName());
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.MAX_VALUE);
    config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 500);
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    final var consumer =
        new KafkaConsumer<>(config, new RecordIdDeserializer(), new RecordDeserializer());
    consumer.subscribe(TOPIC_SUBSCRIPTION_PATTERN);

    return consumer;
  }

  private KafkaContainer newKafkaContainer() {
    final var kafkaImage = DockerImageName.parse("confluentinc/cp-kafka").withTag("5.5.1");
    final var container = new KafkaContainer(kafkaImage);
    final var logConsumer = new Slf4jLogConsumer(newContainerLogger("kafkaContainer"), true);

    return container
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEmbeddedZookeeper()
        .withNetwork(network)
        .withNetworkAliases("kafka")
        .withLogConsumer(logConsumer);
  }

  private void startKafka() {
    kafkaContainer.start();

    // provision Kafka topics - this is difficult at the moment to achieve purely via
    // configuration, so we do it as a pre-step
    final NewTopic topic = new NewTopic("zeebe", 3, (short) 1);
    try (final AdminClient admin =
        AdminClient.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers()))) {
      admin.createTopics(List.of(topic));
    }
  }

  private static Logger newContainerLogger(final String containerName) {
    return LoggerFactory.getLogger(KafkaExporterIT.class.getName() + "." + containerName);
  }
}
