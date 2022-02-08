/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.NoContentException;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.IncomingDeviceMessage;

public class MultiDeviceMessageListProviderTest {

  static byte[] createByteArray(int... bytes) {
    byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      result[i] = (byte) bytes[i];
    }
    return result;
  }

  IncomingDeviceMessage[] tryRead(byte[] bytes) throws Exception {
    MultiDeviceMessageListProvider provider = new MultiDeviceMessageListProvider();
    return provider.readFrom(
      IncomingDeviceMessage[].class,
      IncomingDeviceMessage[].class,
      new Annotation[] {},
      MediaType.valueOf(MultiDeviceMessageListProvider.MEDIA_TYPE),
      new MultivaluedHashMap<>(),
      new ByteArrayInputStream(bytes));
  }

  @Test
  void testInvalidVersion() {
    assertThatThrownBy(() -> tryRead(createByteArray()))
      .isInstanceOf(NoContentException.class);
    assertThatThrownBy(() -> tryRead(createByteArray(0x00)))
      .isInstanceOf(WebApplicationException.class);
    assertThatThrownBy(() -> tryRead(createByteArray(0x59)))
      .isInstanceOf(WebApplicationException.class);
  }

  @Test
  void testBadCount() {
    assertThatThrownBy(() -> tryRead(createByteArray(MultiDeviceMessageListProvider.VERSION)))
      .isInstanceOf(IOException.class);
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION,
        MultiDeviceMessageListProvider.MAX_MESSAGE_COUNT + 1)))
      .isInstanceOf(WebApplicationException.class);
  }

  @Test void testBadDeviceId() {
    // Missing
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1)))
      .isInstanceOf(IOException.class);
    // Unfinished varint
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80)))
      .isInstanceOf(IOException.class);
  }

  @Test void testBadRegistrationId() {
    // Missing
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1)))
      .isInstanceOf(IOException.class);
    // Truncated (fixed u16 value)
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11)))
      .isInstanceOf(IOException.class);
  }

  @Test void testBadType() {
    // Missing
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11, 0x22)))
      .isInstanceOf(IOException.class);
  }

  @Test void testBadMessageLength() {
    // Missing
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11, 0x22, 1)))
      .isInstanceOf(IOException.class);
    // Unfinished varint
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11, 0x22, 1, 0x80)))
      .isInstanceOf(IOException.class);
    // Too big
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11, 0x22, 1, 0x80, 0x80, 0x80, 0x01)))
      .isInstanceOf(WebApplicationException.class);
    // Missing message
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11, 0x22, 1, 0x01)))
      .isInstanceOf(IOException.class);
    // Truncated message
    assertThatThrownBy(() -> tryRead(createByteArray(
        MultiDeviceMessageListProvider.VERSION, 1, 0x80, 1, 0x11, 0x22, 1, 5, 0x01, 0x02)))
      .isInstanceOf(IOException.class);
  }

  @Test
  void readThreeMessages() throws Exception {
    ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
    contentStream.write(MultiDeviceMessageListProvider.VERSION);
    contentStream.write(3);

    contentStream.writeBytes(createByteArray(0x85, 0x02)); // Device ID 0x0105
    contentStream.writeBytes(createByteArray(0x11, 0x05)); // Registration ID 0x1105
    contentStream.write(1);
    contentStream.write(5);
    contentStream.writeBytes(createByteArray(11, 22, 33, 44, 55));

    contentStream.write(0x20); // Device ID 0x20
    contentStream.writeBytes(createByteArray(0x30, 0x20)); // Registration ID 0x3020
    contentStream.write(6);
    contentStream.writeBytes(createByteArray(0x81, 0x01)); // 129 bytes
    contentStream.writeBytes(new byte[129]);

    contentStream.write(1); // Device ID 1
    contentStream.writeBytes(createByteArray(0x00, 0x11)); // Registration ID 0x0011
    contentStream.write(50);
    contentStream.write(0); // empty message for some rateLimitReason

    IncomingDeviceMessage[] messages = tryRead(contentStream.toByteArray());
    assertThat(messages.length).isEqualTo(3);

    assertThat(messages[0].getDeviceId()).isEqualTo(0x0105);
    assertThat(messages[0].getRegistrationId()).isEqualTo(0x1105);
    assertThat(messages[0].getType()).isEqualTo(1);
    assertThat(messages[0].getContent()).containsExactly(11, 22, 33, 44, 55);

    assertThat(messages[1].getDeviceId()).isEqualTo(0x20);
    assertThat(messages[1].getRegistrationId()).isEqualTo(0x3020);
    assertThat(messages[1].getType()).isEqualTo(6);
    assertThat(messages[1].getContent()).containsExactly(new byte[129]);

    assertThat(messages[2].getDeviceId()).isEqualTo(1);
    assertThat(messages[2].getRegistrationId()).isEqualTo(0x0011);
    assertThat(messages[2].getType()).isEqualTo(50);
    assertThat(messages[2].getContent()).isEmpty();
  }

  @Test
  void emptyListIsStillValid() throws Exception {
    ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
    contentStream.write(MultiDeviceMessageListProvider.VERSION);
    contentStream.write(0);

    IncomingDeviceMessage[] messages = tryRead(contentStream.toByteArray());
    assertThat(messages).isEmpty();
  }

}
