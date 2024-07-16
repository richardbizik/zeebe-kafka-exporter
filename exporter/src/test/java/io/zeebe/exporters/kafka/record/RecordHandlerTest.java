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
package io.zeebe.exporters.kafka.record;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.protocol.jackson.ZeebeProtocolModule;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.value.DeploymentRecordValue;
import io.camunda.zeebe.protocol.record.value.ImmutableDeploymentRecordValue;
import io.camunda.zeebe.test.broker.protocol.ProtocolFactory;
import io.zeebe.exporters.kafka.config.RecordConfig;
import io.zeebe.exporters.kafka.config.RecordsConfig;
import io.zeebe.exporters.kafka.serde.RecordId;
import java.util.EnumSet;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
final class RecordHandlerTest {

  private static final RecordConfig DEFAULT_RECORD_CONFIG =
      new RecordConfig(EnumSet.allOf(RecordType.class), "zeebe");
  private final ProtocolFactory recordFactory = new ProtocolFactory();

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new ZeebeProtocolModule());

  @Test
  void shouldTransformRecord() throws JsonProcessingException {
    // given
    final Record<DeploymentRecordValue> record =
        createDeploymentRecord(RecordType.COMMAND);
    final RecordConfig deploymentRecordConfig =
        new RecordConfig(EnumSet.allOf(RecordType.class), "topic");
    final RecordHandler recordHandler = new RecordHandler(newRecordsConfig(RecordType.COMMAND));

    // when
    final ProducerRecord<RecordId, byte[]> transformed = recordHandler.transform(record);

    // then
    assertThat(transformed.topic()).isEqualTo(deploymentRecordConfig.getTopic());
    assertThat(transformed.key())
        .isEqualTo(new RecordId(record.getPartitionId(), record.getPosition()));
    final var recordBytes = objectMapper.writeValueAsBytes(record);
    assertThat(transformed.value()).isEqualTo(recordBytes);
  }

  @Test
  void shouldTestRecordAsNotAllowed() {
    // given
    final Record<DeploymentRecordValue> record =
        createDeploymentRecord(RecordType.COMMAND);
    final RecordHandler recordHandler = new RecordHandler(newRecordsConfig(RecordType.EVENT));

    // when - then
    assertThat(recordHandler.isAllowed(record)).isFalse();
  }

  @Test
  void shouldTestRecordAsAllowed() {
    // given
    final Record<DeploymentRecordValue> record =
        createDeploymentRecord(RecordType.EVENT);
    final RecordHandler recordHandler = new RecordHandler(newRecordsConfig(RecordType.EVENT));

    // when - then
    assertThat(recordHandler.isAllowed(record)).isTrue();
  }

  private RecordsConfig newRecordsConfig(final RecordType allowedType) {
    final RecordConfig recordConfig = new RecordConfig(EnumSet.of(allowedType), "topic");
    return new RecordsConfig(Map.of(ValueType.DEPLOYMENT, recordConfig), DEFAULT_RECORD_CONFIG);
  }

  private Record<DeploymentRecordValue> createDeploymentRecord(final RecordType rt) {
    return recordFactory.generateRecord(ValueType.DEPLOYMENT,
        builder -> builder
        .withRecordType(rt)
        .withTimestamp(System.currentTimeMillis())
        .withIntent(DeploymentIntent.CREATE)
        .withValue(ImmutableDeploymentRecordValue.builder().build())
        .withPartitionId(1));
  }
}
