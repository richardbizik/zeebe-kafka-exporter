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
package io.zeebe.exporters.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import io.camunda.zeebe.exporter.test.ExporterTestConfiguration;
import io.camunda.zeebe.exporter.test.ExporterTestContext;
import io.camunda.zeebe.exporter.test.ExporterTestController;
import io.camunda.zeebe.protocol.record.ImmutableRecord;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.test.broker.protocol.ProtocolFactory;
import io.zeebe.exporters.kafka.config.Config;
import io.zeebe.exporters.kafka.config.parser.ConfigParser;
import io.zeebe.exporters.kafka.config.parser.MockConfigParser;
import io.zeebe.exporters.kafka.config.parser.RawConfigParser;
import io.zeebe.exporters.kafka.config.raw.RawConfig;
import io.zeebe.exporters.kafka.config.raw.RawRecordConfig;
import io.zeebe.exporters.kafka.config.raw.RawRecordsConfig;
import io.zeebe.exporters.kafka.producer.RecordBatchStub;
import io.zeebe.exporters.kafka.record.RecordHandler;
import io.zeebe.exporters.kafka.serde.RecordId;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@SuppressWarnings("rawtypes")
@Execution(ExecutionMode.CONCURRENT)
final class KafkaExporterTest {
  private static final String EXPORTER_ID = "kafka";

  private final RawConfig rawConfig = new RawConfig();
  private final MockConfigParser<RawConfig, Config> mockConfigParser =
      new MockConfigParser<>(new RawConfigParser());
  private final RecordBatchStub.Factory batchStubFactory = new RecordBatchStub.Factory();
  private final ExporterTestController controller = new ExporterTestController();
  private final KafkaExporter exporter = new KafkaExporter(batchStubFactory, mockConfigParser);
  private final ExporterTestContext context =
      new ExporterTestContext().setConfiguration(new ExporterTestConfiguration<>("test", rawConfig));

  private final ProtocolFactory recordFactory = new ProtocolFactory();

  @Test
  void shouldAddRecordToBatchOnExport() throws Exception {
    // given
    rawConfig.maxBatchSize = 4;
    exporter.configure(context);
    exporter.open(controller);
    final List<Record> records =
          List.of(
              newRecord(1, ValueType.PROCESS_INSTANCE),
              newRecord(2, ValueType.PROCESS_INSTANCE),
              newRecord(3, ValueType.PROCESS_INSTANCE),
              newRecord(1, ValueType.PROCESS_INSTANCE));
    // when
    records.forEach(exporter::export);

    // then
    final var expectedIds =
        records.stream()
            .map(r -> new RecordId(r.getPartitionId(), r.getPosition()))
            .collect(Collectors.toList());
    assertThat(batchStubFactory.stub.getPendingRecords())
        .as("the records were added to the batch in order")
        .extracting(ProducerRecord::key)
        .containsExactlyElementsOf(expectedIds);
    assertThat(batchStubFactory.stub.getFlushedRecords())
        .as("no records were flushed yet")
        .isEmpty();
  }

  @Test
  void shouldUseCorrectSerializer() throws Exception {
    // given
    exporter.configure(context);
    exporter.open(controller);
    final var recordHandler = new RecordHandler(mockConfigParser.config.getRecords());

    // when
    final var record = recordFactory.generateRecord(ValueType.FORM);
    exporter.export(record);

    // then
    final var expectedRecord = recordHandler.transform(record);
    assertThat(batchStubFactory.stub.getPendingRecords())
        .as("the serialized record was added to the batch")
        .extracting("topic", "key", "value")
        .containsExactly(
            tuple(expectedRecord.topic(), expectedRecord.key(), expectedRecord.value()));
  }

  @Test
  void shouldSkipDisallowedRecords() throws Exception {
    // given
    rawConfig.records = new RawRecordsConfig();
    rawConfig.records.deployment = new RawRecordConfig();
    rawConfig.records.deployment.type = RecordType.COMMAND.toString();
    mockConfigParser.forceParse(rawConfig);
    final var context = new ExporterTestContext().setConfiguration(new ExporterTestConfiguration<>("test", rawConfig));
    exporter.configure(context);
    exporter.open(controller);
    final var record = recordFactory.generateRecord(ValueType.DEPLOYMENT);

    // when
    exporter.export(record);

    // then
    assertThat(batchStubFactory.stub.getPendingRecords())
        .as("disallowed record should not be added to the batch")
        .isEmpty();
  }

  @Test
  void shouldFlushOnScheduledTask() throws Exception {
    // given
    rawConfig.maxBatchSize = 5;
    final var context = new ExporterTestContext().setConfiguration(new ExporterTestConfiguration<>("test", rawConfig));
    exporter.configure(context);
    exporter.open(controller);
    final var records =
          List.of(
              recordFactory.generateRecord(ValueType.DEPLOYMENT),
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.DEPLOYMENT),
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.JOB));
    // when
    records.forEach(exporter::export);
    triggerFlushTask();

    // then
    final var expectedIds =
        records.stream()
            .map(r -> new RecordId(r.getPartitionId(), r.getPosition()))
            .collect(Collectors.toList());
    assertThat(batchStubFactory.stub.getFlushedRecords())
        .as("the records were added to the batch in order")
        .extracting(ProducerRecord::key)
        .containsExactlyElementsOf(expectedIds);
    assertThat(batchStubFactory.stub.getPendingRecords())
        .as("no pending records after flush")
        .isEmpty();
  }

  @Test
  void shouldUpdatePositionOnFlush() throws Exception {
    // given
    exporter.configure(context);
    exporter.open(controller);
    final var records =
          List.of(
              recordFactory.generateRecord(ValueType.DEPLOYMENT),
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.VARIABLE),
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.JOB));
    // when
    records.forEach(exporter::export);
    triggerFlushTask();

    // then
    assertThat(controller.getLastExportedRecordPosition())
        .as("position should be updated since after flush")
        .isEqualTo(records.get(4).getPosition());
  }

  @Test
  void shouldRescheduleFlushTaskEvenOnException() throws Exception {
    // given
    exporter.configure(context);
    exporter.open(controller);
    final var records =
          List.of(
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.JOB));
    // when
    records.forEach(exporter::export);
    batchStubFactory.stub.flushException = new RuntimeException("failed to flush");
    assertThatThrownBy(this::triggerFlushTask).isEqualTo(batchStubFactory.stub.flushException);
    batchStubFactory.stub.flushException = null;
    triggerFlushTask();

    // then
    assertThat(controller.getLastExportedRecordPosition())
        .as("position should be updated since we managed to flush after the second try")
        .isEqualTo(records.get(1).getPosition());
  }

  @Test
  void shouldFlushBatchOnClose() throws Exception {
    // given
    exporter.configure(context);
    exporter.open(controller);
    final var records =
          List.of(
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.JOB));
    // when
    records.forEach(exporter::export);
    exporter.close();

    // then
    assertThat(controller.getLastExportedRecordPosition())
        .as("position should be updated since we managed to flush after the second try")
        .isEqualTo(records.get(1).getPosition());
    assertThat(batchStubFactory.stub.isClosed())
        .as("batch should be closed on exporter close")
        .isTrue();
  }

  @Test
  void shouldRescheduleFlush() throws Exception {
    // given
    exporter.configure(context);
    exporter.open(controller);
    final var records =
          List.of(
              recordFactory.generateRecord(ValueType.PROCESS_INSTANCE),
              recordFactory.generateRecord(ValueType.JOB));
    // when
    triggerFlushTask();
    records.forEach(exporter::export);
    triggerFlushTask();

    // then
    assertThat(controller.getLastExportedRecordPosition())
        .as("position should be updated after triggering the second flush task")
        .isEqualTo(records.get(1).getPosition());
  }

  private void triggerFlushTask() {
    mockConfigParser.parse(rawConfig);
    controller.runScheduledTasks(mockConfigParser.config.getFlushInterval());
  }

  private static Record<?> newRecord(final int partitionId, final ValueType valueType) {
    return ImmutableRecord.builder()
        .withPartitionId(partitionId)
        .withValueType(valueType)
        .withRecordType(RecordType.EVENT)
        .build();
  }
}
