/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.vdurmont.semver4j.Semver;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

class ClientReleasesTest {

  private ClientReleases clientReleases;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.CLIENT_RELEASES);

  @BeforeEach
  void setUp() {
    clientReleases = new ClientReleases(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.CLIENT_RELEASES.tableName());
  }

  @Test
  void getClientReleases() {
    final Instant releaseTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    final Instant expiration = releaseTimestamp.plusSeconds(60);

    storeClientRelease("IOS", "1.2.3", releaseTimestamp, expiration);
    storeClientRelease("IOS", "not-a-valid-version", releaseTimestamp, expiration);
    storeClientRelease("ANDROID", "4.5.6", releaseTimestamp, expiration);
    storeClientRelease("UNRECOGNIZED_PLATFORM", "7.8.9", releaseTimestamp, expiration);

    final Map<ClientPlatform, Map<Semver, ClientRelease>> expectedVersions = Map.of(
        ClientPlatform.IOS, Map.of(new Semver("1.2.3"), new ClientRelease(ClientPlatform.IOS, new Semver("1.2.3"), releaseTimestamp, expiration)),
        ClientPlatform.ANDROID, Map.of(new Semver("4.5.6"), new ClientRelease(ClientPlatform.ANDROID, new Semver("4.5.6"), releaseTimestamp, expiration)));

    assertEquals(expectedVersions, clientReleases.getClientReleases());
  }

  private void storeClientRelease(final String platform, final String version, final Instant release, final Instant expiration) {
    DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
        .tableName(DynamoDbExtensionSchema.Tables.CLIENT_RELEASES.tableName())
        .item(Map.of(
            ClientReleases.ATTR_PLATFORM, AttributeValue.builder().s(platform).build(),
            ClientReleases.ATTR_VERSION, AttributeValue.builder().s(version).build(),
            ClientReleases.ATTR_RELEASE_TIMESTAMP,
            AttributeValue.builder().n(String.valueOf(release.getEpochSecond())).build(),
            ClientReleases.ATTR_EXPIRATION,
            AttributeValue.builder().n(String.valueOf(expiration.getEpochSecond())).build()))
        .build());
  }
}
