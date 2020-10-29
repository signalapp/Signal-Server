/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import com.google.protobuf.ByteString;
import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Messages;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@RunWith(JUnitParamsRunner.class)
public class MessagesTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("messagedb.xml"));

  private Messages messages;

  private long serialTimestamp = 0;

  @Before
  public void setupAccountsDao() {
    this.messages = new Messages(new FaultTolerantDatabase("messages-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  }

  @Test
  public void testStore() throws SQLException {
    Envelope envelope = generateEnvelope();

    messages.store(List.of(envelope), "+14151112222", 1);

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM messages WHERE destination = ?");
    statement.setString(1, "+14151112222");

    ResultSet resultSet = statement.executeQuery();
    assertThat(resultSet.next()).isTrue();

    assertThat(resultSet.getString("guid")).isEqualTo(envelope.getServerGuid());
    assertThat(resultSet.getInt("type")).isEqualTo(envelope.getType().getNumber());
    assertThat(resultSet.getString("relay")).isNullOrEmpty();
    assertThat(resultSet.getLong("timestamp")).isEqualTo(envelope.getTimestamp());
    assertThat(resultSet.getLong("server_timestamp")).isEqualTo(envelope.getServerTimestamp());
    assertThat(resultSet.getString("source")).isEqualTo(envelope.getSource());
    assertThat(resultSet.getLong("source_device")).isEqualTo(envelope.getSourceDevice());
    assertThat(resultSet.getBytes("message")).isEqualTo(envelope.getLegacyMessage().toByteArray());
    assertThat(resultSet.getBytes("content")).isEqualTo(envelope.getContent().toByteArray());
    assertThat(resultSet.getString("destination")).isEqualTo("+14151112222");
    assertThat(resultSet.getLong("destination_device")).isEqualTo(1);

    assertThat(resultSet.next()).isFalse();
  }

  @Test
  @Parameters(method = "argumentsForTestStoreSealedSenderBatch")
  public void testStoreSealedSenderBatch(final List<Boolean> sealedSenderSequence) throws Exception {
    final String   destinationNumber      = "+14151234567";

    final List<Envelope> envelopes = sealedSenderSequence.stream()
            .map(sealedSender -> {
              if (sealedSender) {
                return generateEnvelope().toBuilder().clearSourceUuid().clearSource().clearSourceDevice().build();
              } else {
                return generateEnvelope().toBuilder().setSourceUuid(UUID.randomUUID().toString()).setSource("+18005551234").setSourceDevice(4).build();
              }
            }).collect(Collectors.toList());

    messages.store(envelopes, destinationNumber, 1);

    final Queue<Envelope> expectedMessages = new ArrayDeque<>(envelopes);

    try (final PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM messages WHERE destination = ?")) {
      statement.setString(1, destinationNumber);
      
      try (final ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next() && !expectedMessages.isEmpty()) {
          assertRowEqualsEnvelope(resultSet, destinationNumber, expectedMessages.poll());
        }

        assertThat(resultSet.next()).isFalse();
        assertThat(expectedMessages.isEmpty());
      }
    }
  }

  private static Object argumentsForTestStoreSealedSenderBatch() {
    return new Object[] {
            List.of(true),
            List.of(false),
            List.of(true, false),
            List.of(false, true)
    };
  }

  private void assertRowEqualsEnvelope(final ResultSet resultSet, final String expectedDestination, final Envelope expectedMessage) throws SQLException {
    assertThat(resultSet.getString("guid")).isEqualTo(expectedMessage.getServerGuid());
    assertThat(resultSet.getInt("type")).isEqualTo(expectedMessage.getType().getNumber());
    assertThat(resultSet.getString("relay")).isNullOrEmpty();
    assertThat(resultSet.getLong("timestamp")).isEqualTo(expectedMessage.getTimestamp());
    assertThat(resultSet.getLong("server_timestamp")).isEqualTo(expectedMessage.getServerTimestamp());
    assertThat(resultSet.getBytes("message")).isEqualTo(expectedMessage.getLegacyMessage().toByteArray());
    assertThat(resultSet.getBytes("content")).isEqualTo(expectedMessage.getContent().toByteArray());
    assertThat(resultSet.getString("destination")).isEqualTo(expectedDestination);
    assertThat(resultSet.getLong("destination_device")).isEqualTo(1);

    if (expectedMessage.hasSource()) {
      assertThat(resultSet.getString("source")).isEqualTo(expectedMessage.getSource());
    } else {
      assertThat(resultSet.getString("source")).isNullOrEmpty();
    }

    if (expectedMessage.hasSourceDevice()) {
      assertThat(resultSet.getLong("source_device")).isEqualTo(expectedMessage.getSourceDevice());
    } else {
      assertThat(resultSet.getLong("source_device")).isEqualTo(0);
    }

    if (expectedMessage.hasSourceUuid()) {
      assertThat(resultSet.getString("source_uuid")).isEqualTo(expectedMessage.getSourceUuid());
    } else {
      assertThat(resultSet.getString("source_uuid")).isNull();
    }
  }

  @Test
  public void testLoad() {
    List<Envelope> inserted = insertRandom("+14151112222", 1);

    inserted.sort(Comparator.comparingLong(Envelope::getTimestamp));

    List<OutgoingMessageEntity> retrieved = messages.load("+14151112222", 1);

    assertThat(retrieved.size()).isEqualTo(inserted.size());

    for (int i=0;i<retrieved.size();i++) {
      verifyExpected(retrieved.get(i), inserted.get(i), UUID.fromString(inserted.get(i).getServerGuid()));
    }

  }

  @Test
  public void removeBySourceDestinationTimestamp() {
    List<Envelope>                  inserted  = insertRandom("+14151112222", 1);
    List<Envelope>                  unrelated = insertRandom("+14151114444", 3);
    Envelope                        toRemove  = inserted.remove(new Random(System.currentTimeMillis()).nextInt(inserted.size() - 1));
    Optional<OutgoingMessageEntity> removed   = messages.remove("+14151112222", 1, toRemove.getSource(), toRemove.getTimestamp());

    assertThat(removed.isPresent()).isTrue();
    verifyExpected(removed.get(), toRemove, UUID.fromString(toRemove.getServerGuid()));

    verifyInTact(inserted, "+14151112222", 1);
    verifyInTact(unrelated, "+14151114444", 3);
  }

  @Test
  public void removeByDestinationGuid() {
    List<Envelope>                  unrelated = insertRandom("+14151113333", 2);
    List<Envelope>                  inserted  = insertRandom("+14151112222", 1);
    Envelope                        toRemove  = inserted.remove(new Random(System.currentTimeMillis()).nextInt(inserted.size() - 1));
    Optional<OutgoingMessageEntity> removed   = messages.remove("+14151112222", UUID.fromString(toRemove.getServerGuid()));

    assertThat(removed.isPresent()).isTrue();
    verifyExpected(removed.get(), toRemove, UUID.fromString(toRemove.getServerGuid()));

    verifyInTact(inserted, "+14151112222", 1);
    verifyInTact(unrelated, "+14151113333", 2);
  }

  @Test
  public void removeByDestinationRowId() {
    List<Envelope> unrelatedInserted = insertRandom("+14151111111", 1);
    List<Envelope> inserted          = insertRandom("+14151112222", 1);

    inserted.sort(Comparator.comparingLong(Envelope::getTimestamp));

    List<OutgoingMessageEntity> retrieved = messages.load("+14151112222", 1);

    int toRemoveIndex = new Random(System.currentTimeMillis()).nextInt(inserted.size() - 1);

    inserted.remove(toRemoveIndex);

    messages.remove("+14151112222", retrieved.get(toRemoveIndex).getId());

    verifyInTact(inserted, "+14151112222", 1);
    verifyInTact(unrelatedInserted, "+14151111111", 1);
  }

  @Test
  public void testLoadEmpty() {
    insertRandom("+14151112222", 1);
    assertThat(messages.load("+14159999999", 1).isEmpty()).isTrue();
  }

  @Test
  public void testClearDestination() {
    insertRandom("+14151112222", 1);
    insertRandom("+14151112222", 2);

    List<Envelope> unrelated = insertRandom("+14151111111", 1);

    messages.clear("+14151112222");

    assertThat(messages.load("+14151112222", 1).isEmpty()).isTrue();

    verifyInTact(unrelated, "+14151111111", 1);
  }

  @Test
  public void testClearDestinationDevice() {
    insertRandom("+14151112222", 1);
    List<Envelope> inserted = insertRandom("+14151112222", 2);

    List<Envelope> unrelated = insertRandom("+14151111111", 1);

    messages.clear("+14151112222", 1);

    assertThat(messages.load("+14151112222", 1).isEmpty()).isTrue();

    verifyInTact(inserted, "+14151112222", 2);
    verifyInTact(unrelated, "+14151111111", 1);
  }

  @Test
  public void testVacuum() {
    List<Envelope> inserted = insertRandom("+14151112222", 2);
    messages.vacuum();
    verifyInTact(inserted, "+14151112222", 2);
  }

  private List<Envelope> insertRandom(String destination, int destinationDevice) {
    List<Envelope> inserted = new ArrayList<>(50);

    for (int i=0;i<50;i++) {
      inserted.add(generateEnvelope());
    }

    messages.store(inserted, destination, destinationDevice);

    return inserted;
  }

  private void verifyInTact(List<Envelope> inserted, String destination, int destinationDevice) {
    inserted.sort(Comparator.comparingLong(Envelope::getTimestamp));

    List<OutgoingMessageEntity> retrieved = messages.load(destination, destinationDevice);

    assertThat(retrieved.size()).isEqualTo(inserted.size());

    for (int i=0;i<retrieved.size();i++) {
      verifyExpected(retrieved.get(i), inserted.get(i), UUID.fromString(inserted.get(i).getServerGuid()));
    }
  }


  private void verifyExpected(OutgoingMessageEntity retrieved, Envelope inserted, UUID guid) {
    assertThat(retrieved.getTimestamp()).isEqualTo(inserted.getTimestamp());
    assertThat(retrieved.getSource()).isEqualTo(inserted.getSource());
    assertThat(retrieved.getRelay()).isEqualTo(inserted.getRelay());
    assertThat(retrieved.getType()).isEqualTo(inserted.getType().getNumber());
    assertThat(retrieved.getContent()).isEqualTo(inserted.getContent().toByteArray());
    assertThat(retrieved.getMessage()).isEqualTo(inserted.getLegacyMessage().toByteArray());
    assertThat(retrieved.getServerTimestamp()).isEqualTo(inserted.getServerTimestamp());
    assertThat(retrieved.getGuid()).isEqualTo(guid);
    assertThat(retrieved.getSourceDevice()).isEqualTo(inserted.getSourceDevice());
  }

  private Envelope generateEnvelope() {
    Random random = new Random();
    byte[] content = new byte[256];
    byte[] legacy = new byte[200];

    Arrays.fill(content, (byte)random.nextInt(255));
    Arrays.fill(legacy, (byte)random.nextInt(255));

    return Envelope.newBuilder()
                   .setServerGuid(UUID.randomUUID().toString())
                   .setSourceDevice(random.nextInt(10000))
                   .setSource("testSource" + random.nextInt())
                   .setTimestamp(serialTimestamp++)
                   .setServerTimestamp(serialTimestamp++)
                   .setLegacyMessage(ByteString.copyFrom(legacy))
                   .setContent(ByteString.copyFrom(content))
                   .setType(Envelope.Type.CIPHERTEXT)
                   .setServerGuid(UUID.randomUUID().toString())
                   .build();
  }
}
