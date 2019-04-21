package org.whispersystems.textsecuregcm.tests.storage;

import com.google.protobuf.ByteString;
import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Messages;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class MessagesTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("messagedb.xml"));

  private Messages messages;

  @Before
  public void setupAccountsDao() {
    this.messages = new Messages(new FaultTolerantDatabase("messages-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  }

  @Test
  public void testStore() throws SQLException {
    Envelope envelope = generateEnvelope();
    UUID     guid     = UUID.randomUUID();

    messages.store(guid, envelope, "+14151112222", 1);

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM messages WHERE destination = ?");
    statement.setString(1, "+14151112222");

    ResultSet resultSet = statement.executeQuery();
    assertThat(resultSet.next()).isTrue();

    assertThat(resultSet.getString("guid")).isEqualTo(guid.toString());
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
  public void testLoad() {
    List<MessageToStore> inserted = new ArrayList<>(50);

    for (int i=0;i<50;i++) {
      MessageToStore message = generateMessageToStore();
      inserted.add(message);

      messages.store(message.guid, message.envelope, "+14151112222", 1);
    }

    inserted.sort(Comparator.comparingLong(o -> o.envelope.getTimestamp()));

    List<OutgoingMessageEntity> retrieved = messages.load("+14151112222", 1);

    assertThat(retrieved.size()).isEqualTo(inserted.size());

    for (int i=0;i<retrieved.size();i++) {
      verifyExpected(retrieved.get(i), inserted.get(i).envelope, inserted.get(i).guid);
    }

  }

  @Test
  public void removeBySourceDestinationTimestamp() {
    List<MessageToStore>            inserted = insertRandom("+14151112222", 1);
    List<MessageToStore>            unrelated = insertRandom("+14151114444", 3);
    MessageToStore                  toRemove = inserted.remove(new Random(System.currentTimeMillis()).nextInt(inserted.size() - 1));
    Optional<OutgoingMessageEntity> removed  = messages.remove("+14151112222", 1, toRemove.envelope.getSource(), toRemove.envelope.getTimestamp());

    assertThat(removed.isPresent()).isTrue();
    verifyExpected(removed.get(), toRemove.envelope, toRemove.guid);

    verifyInTact(inserted, "+14151112222", 1);
    verifyInTact(unrelated, "+14151114444", 3);
  }

  @Test
  public void removeByDestinationGuid() {
    List<MessageToStore>            unrelated = insertRandom("+14151113333", 2);
    List<MessageToStore>            inserted = insertRandom("+14151112222", 1);
    MessageToStore                  toRemove = inserted.remove(new Random(System.currentTimeMillis()).nextInt(inserted.size() - 1));
    Optional<OutgoingMessageEntity> removed  = messages.remove("+14151112222", toRemove.guid);

    assertThat(removed.isPresent()).isTrue();
    verifyExpected(removed.get(), toRemove.envelope, toRemove.guid);

    verifyInTact(inserted, "+14151112222", 1);
    verifyInTact(unrelated, "+14151113333", 2);
  }

  @Test
  public void removeByDestinationRowId() {
    List<MessageToStore> unrelatedInserted = insertRandom("+14151111111", 1);
    List<MessageToStore> inserted          = insertRandom("+14151112222", 1);

    inserted.sort(Comparator.comparingLong(o -> o.envelope.getTimestamp()));

    List<OutgoingMessageEntity> retrieved = messages.load("+14151112222", 1);

    int toRemoveIndex = new Random(System.currentTimeMillis()).nextInt(inserted.size() - 1);

    inserted.remove(toRemoveIndex);

    messages.remove("+14151112222", retrieved.get(toRemoveIndex).getId());

    verifyInTact(inserted, "+14151112222", 1);
    verifyInTact(unrelatedInserted, "+14151111111", 1);
  }

  @Test
  public void testLoadEmpty() {
    List<MessageToStore> inserted = insertRandom("+14151112222", 1);
    List<OutgoingMessageEntity> loaded = messages.load("+14159999999", 1);
    assertThat(loaded.isEmpty()).isTrue();
  }

  @Test
  public void testClearDestination() {
    insertRandom("+14151112222", 1);
    insertRandom("+14151112222", 2);

    List<MessageToStore> unrelated = insertRandom("+14151111111", 1);

    messages.clear("+14151112222");

    assertThat(messages.load("+14151112222", 1).isEmpty()).isTrue();

    verifyInTact(unrelated, "+14151111111", 1);
  }

  @Test
  public void testClearDestinationDevice() {
    insertRandom("+14151112222", 1);
    List<MessageToStore> inserted = insertRandom("+14151112222", 2);

    List<MessageToStore> unrelated = insertRandom("+14151111111", 1);

    messages.clear("+14151112222", 1);

    assertThat(messages.load("+14151112222", 1).isEmpty()).isTrue();

    verifyInTact(inserted, "+14151112222", 2);
    verifyInTact(unrelated, "+14151111111", 1);
  }

  @Test
  public void testVacuum() {
    List<MessageToStore> inserted = insertRandom("+14151112222", 2);
    messages.vacuum();
    verifyInTact(inserted, "+14151112222", 2);
  }

  private List<MessageToStore> insertRandom(String destination, int destinationDevice) {
    List<MessageToStore> inserted = new ArrayList<>(50);

    for (int i=0;i<50;i++) {
      MessageToStore message = generateMessageToStore();
      inserted.add(message);

      messages.store(message.guid, message.envelope, destination, destinationDevice);
    }

    return inserted;
  }

  private void verifyInTact(List<MessageToStore> inserted, String destination, int destinationDevice) {
    inserted.sort(Comparator.comparingLong(o -> o.envelope.getTimestamp()));

    List<OutgoingMessageEntity> retrieved = messages.load(destination, destinationDevice);

    assertThat(retrieved.size()).isEqualTo(inserted.size());

    for (int i=0;i<retrieved.size();i++) {
      verifyExpected(retrieved.get(i), inserted.get(i).envelope, inserted.get(i).guid);
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

  private MessageToStore generateMessageToStore() {
    return new MessageToStore(UUID.randomUUID(), generateEnvelope());
  }

  private Envelope generateEnvelope() {
    Random random = new Random();
    byte[] content = new byte[256];
    byte[] legacy = new byte[200];

    Arrays.fill(content, (byte)random.nextInt(255));
    Arrays.fill(legacy, (byte)random.nextInt(255));

    return Envelope.newBuilder()
                   .setSourceDevice(random.nextInt(10000))
                   .setSource("testSource" + random.nextInt())
                   .setTimestamp(random.nextInt(100000))
                   .setServerTimestamp(random.nextInt(100000))
                   .setLegacyMessage(ByteString.copyFrom(legacy))
                   .setContent(ByteString.copyFrom(content))
                   .setType(Envelope.Type.CIPHERTEXT)
                   .setServerGuid(UUID.randomUUID().toString())
                   .build();
  }

  private static class MessageToStore {
    private final UUID guid;
    private final Envelope envelope;

    private MessageToStore(UUID guid, Envelope envelope) {
      this.guid = guid;
      this.envelope = envelope;
    }
  }
}
