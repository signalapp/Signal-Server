/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** AwsAV provides static helper methods for working with AWS AttributeValues. */
public class AttributeValues {

  public static AttributeValue fromString(String value) {
    return AttributeValue.builder().s(value).build();
  }

  public static AttributeValue fromLong(long value) {
    return AttributeValue.builder().n(Long.toString(value)).build();
  }

  public static AttributeValue fromInt(int value) {
    return AttributeValue.builder().n(Integer.toString(value)).build();
  }

  public static AttributeValue fromByteArray(byte[] value) {
    return AttributeValues.fromSdkBytes(SdkBytes.fromByteArray(value));
  }

  public static AttributeValue fromByteBuffer(ByteBuffer value) {
    return AttributeValues.fromSdkBytes(SdkBytes.fromByteBuffer(value));
  }

  public static AttributeValue fromUUID(UUID uuid) {
    return AttributeValues.fromSdkBytes(SdkBytes.fromByteArrayUnsafe(UUIDUtil.toBytes(uuid)));
  }

  public static AttributeValue fromSdkBytes(SdkBytes value) {
    return AttributeValue.builder().b(value).build();
  }

  public static AttributeValue fromBoolean(boolean value) {
    return AttributeValue.builder().bool(value).build();
  }

  private static int toInt(AttributeValue av) {
    return Integer.parseInt(av.n());
  }

  private static long toLong(AttributeValue av) {
    return Long.parseLong(av.n());
  }

  private static UUID toUUID(AttributeValue av) {
    return UUIDUtil.fromBytes(av.b().asByteArrayUnsafe());  // We're guaranteed not to modify the byte array
  }

  private static byte[] toByteArray(AttributeValue av) {
    return av.b().asByteArray();
  }

  private static String toString(AttributeValue av) {
    return av.s();
  }

  public static Optional<AttributeValue> get(Map<String, AttributeValue> item, String key) {
    return Optional.ofNullable(item.get(key));
  }

  public static int getInt(Map<String, AttributeValue> item, String key, int defaultValue) {
    return AttributeValues.get(item, key).map(AttributeValues::toInt).orElse(defaultValue);
  }

  public static String getString(Map<String, AttributeValue> item, String key, String defaultValue) {
    return AttributeValues.get(item, key).map(AttributeValues::toString).orElse(defaultValue);
  }

  public static long getLong(Map<String, AttributeValue> item, String key, long defaultValue) {
    return AttributeValues.get(item, key).map(AttributeValues::toLong).orElse(defaultValue);
  }

  public static byte[] getByteArray(Map<String, AttributeValue> item, String key, byte[] defaultValue) {
    return AttributeValues.get(item, key).map(AttributeValues::toByteArray).orElse(defaultValue);
  }

  public static UUID getUUID(Map<String, AttributeValue> item, String key, UUID defaultValue) {
    return AttributeValues.get(item, key).map(AttributeValues::toUUID).orElse(defaultValue);
  }
}
