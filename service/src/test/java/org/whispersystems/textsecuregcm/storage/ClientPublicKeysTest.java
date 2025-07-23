package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;

class ClientPublicKeysTest {

  private ClientPublicKeys clientPublicKeys;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.CLIENT_PUBLIC_KEYS);

  @BeforeEach
  void setUp() {
    clientPublicKeys = new ClientPublicKeys(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.CLIENT_PUBLIC_KEYS.tableName());
  }

  @Test
  void buildTransactWriteItemForInsertionAndDeletion() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;
    final ECPublicKey publicKey = ECKeyPair.generate().getPublicKey();

    assertEquals(Optional.empty(), clientPublicKeys.findPublicKey(accountIdentifier, deviceId).join());

    DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(clientPublicKeys.buildTransactWriteItemForInsertion(accountIdentifier, deviceId, publicKey))
        .build());

    assertEquals(Optional.of(publicKey), clientPublicKeys.findPublicKey(accountIdentifier, deviceId).join());

    DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(clientPublicKeys.buildTransactWriteItemForDeletion(accountIdentifier, deviceId))
        .build());

    assertEquals(Optional.empty(), clientPublicKeys.findPublicKey(accountIdentifier, deviceId).join());
  }

  @Test
  void setPublicKey() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;
    final ECPublicKey publicKey = ECKeyPair.generate().getPublicKey();

    assertEquals(Optional.empty(), clientPublicKeys.findPublicKey(accountIdentifier, deviceId).join());

    clientPublicKeys.setPublicKey(accountIdentifier, deviceId, publicKey).join();

    assertEquals(Optional.of(publicKey), clientPublicKeys.findPublicKey(accountIdentifier, deviceId).join());
  }
}
