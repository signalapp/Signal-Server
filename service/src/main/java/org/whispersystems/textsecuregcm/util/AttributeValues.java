/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** AwsAV provides static helper methods for working with AWS AttributeValues. */
public class AttributeValues {

  // Clear-type methods

  public static AttributeValue b(byte[] value) {
    return AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build();
  }

  public static AttributeValue b(ByteBuffer value) {
    return AttributeValue.builder().b(SdkBytes.fromByteBuffer(value)).build();
  }

  public static AttributeValue b(UUID value) {
    return b(UUIDUtil.toByteBuffer(value));
  }

  public static AttributeValue n(long value) {
    return AttributeValue.builder().n(String.valueOf(value)).build();
  }

  public static AttributeValue s(String value) {
    return AttributeValue.builder().s(value).build();
  }

  public static AttributeValue m(Map<String, AttributeValue> value) {
    return AttributeValue.builder().m(value).build();
  }

  // More opinionated methods

  public static AttributeValue fromString(String value) {
    return AttributeValue.builder().s(value).build();
  }

  public static AttributeValue fromLong(long value) {
    return AttributeValue.builder().n(Long.toString(value)).build();
  }

  public static AttributeValue fromBool(boolean value) { return AttributeValue.builder().bool(value).build(); }

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

  private static boolean toBool(AttributeValue av) {
    return av.bool();
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

  public static boolean getBool(Map<String, AttributeValue> item, String key, boolean defaultValue) {
    return AttributeValues.get(item, key).map(AttributeValues::toBool).orElse(defaultValue);
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
    return AttributeValues.get(item, key).filter(av -> av.b() != null).map(AttributeValues::toUUID).orElse(defaultValue);
  }

  /**
   * Extracts a byte array from an {@link AttributeValue} that may be either a byte array or a base64-encoded string.
   *
   * @param attributeValue the {@code AttributeValue} from which to extract a byte array
   *
   * @return the byte array represented by the given {@code AttributeValue}
   */
  @VisibleForTesting
  public static byte[] extractByteArray(final AttributeValue attributeValue, final String counterName) {
    if (attributeValue.b() != null) {
      Metrics.counter(counterName, "format", "bytes").increment();
      return attributeValue.b().asByteArray();
    } else if (StringUtils.isNotBlank(attributeValue.s())) {
      Metrics.counter(counterName, "format", "string").increment();
      return Base64.getDecoder().decode(attributeValue.s());
    }

    throw new IllegalArgumentException("Attribute value has neither a byte array nor a string value");
  }
}
