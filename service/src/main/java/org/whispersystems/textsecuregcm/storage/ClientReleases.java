/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.vdurmont.semver4j.Semver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class ClientReleases {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;

  public static final String ATTR_PLATFORM = "P";
  public static final String ATTR_VERSION = "V";
  public static final String ATTR_RELEASE_TIMESTAMP = "T";
  public static final String ATTR_EXPIRATION = "E";

  private static final Logger logger = LoggerFactory.getLogger(ClientReleases.class);

  public ClientReleases(final DynamoDbAsyncClient dynamoDbAsyncClient, final String tableName) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
  }

  public Map<ClientPlatform, Map<Semver, ClientRelease>> getClientReleases() {
    return Collections.unmodifiableMap(
        Flux.from(dynamoDbAsyncClient.scanPaginator(ScanRequest.builder()
                    .tableName(tableName)
                    .build())
                .items())
            .mapNotNull(ClientReleases::releaseFromItem)
            .groupBy(ClientRelease::platform)
            .flatMap(groupedFlux -> groupedFlux.collectMap(ClientRelease::version)
                .map(releasesByVersion -> Tuples.of(groupedFlux.key(), releasesByVersion)))
            .collectMap(Tuple2::getT1, Tuple2::getT2)
            .blockOptional()
            .orElseGet(Collections::emptyMap));
  }

  @Nullable
  static ClientRelease releaseFromItem(final Map<String, AttributeValue> item) {
    try {
      final ClientPlatform platform = ClientPlatform.valueOf(item.get(ATTR_PLATFORM).s());
      final Semver version = new Semver(item.get(ATTR_VERSION).s());
      final Instant release = Instant.ofEpochSecond(Long.parseLong(item.get(ATTR_RELEASE_TIMESTAMP).n()));
      final Instant expiration = Instant.ofEpochSecond(Long.parseLong(item.get(ATTR_EXPIRATION).n()));

      return new ClientRelease(platform, version, release, expiration);
    } catch (final Exception e) {
      logger.warn("Failed to parse client release item", e);
      return null;
    }
  }
}
