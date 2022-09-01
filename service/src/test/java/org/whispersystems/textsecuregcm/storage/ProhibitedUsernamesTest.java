/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class ProhibitedUsernamesTest {

  private static final String RESERVED_USERNAMES_TABLE_NAME = "reserved_usernames_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(RESERVED_USERNAMES_TABLE_NAME)
      .hashKey(ProhibitedUsernames.KEY_PATTERN)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(ProhibitedUsernames.KEY_PATTERN)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  private static final UUID RESERVED_FOR_UUID = UUID.randomUUID();

  private ProhibitedUsernames prohibitedUsernames;

  @BeforeEach
  void setUp() {
    prohibitedUsernames =
        new ProhibitedUsernames(dynamoDbExtension.getDynamoDbClient(), RESERVED_USERNAMES_TABLE_NAME);
  }

  @ParameterizedTest
  @MethodSource
  void isReserved(final String username, final UUID uuid, final boolean expectReserved) {
    prohibitedUsernames.prohibitUsername(".*myusername.*", RESERVED_FOR_UUID);
    prohibitedUsernames.prohibitUsername("^foobar$", RESERVED_FOR_UUID);

    assertEquals(expectReserved, prohibitedUsernames.isProhibited(username, uuid));
  }

  private static Stream<Arguments> isReserved() {
    return Stream.of(
        Arguments.of("myusername", UUID.randomUUID(), true),
        Arguments.of("myusername", RESERVED_FOR_UUID, false),
        Arguments.of("thyusername", UUID.randomUUID(), false),
        Arguments.of("somemyusername", UUID.randomUUID(), true),
        Arguments.of("myusernamesome", UUID.randomUUID(), true),
        Arguments.of("somemyusernamesome", UUID.randomUUID(), true),
        Arguments.of("MYUSERNAME", UUID.randomUUID(), true),
        Arguments.of("foobar", UUID.randomUUID(), true),
        Arguments.of("foobar", RESERVED_FOR_UUID, false),
        Arguments.of("somefoobar", UUID.randomUUID(), false),
        Arguments.of("foobarsome", UUID.randomUUID(), false),
        Arguments.of("somefoobarsome", UUID.randomUUID(), false),
        Arguments.of("FOOBAR", UUID.randomUUID(), true));
  }
}
