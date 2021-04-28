/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import io.dropwizard.util.DataSizeUnit;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.UUID;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NoContentException;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.whispersystems.textsecuregcm.entities.MultiRecipientMessage;

@Provider
@Consumes(MultiRecipientMessageProvider.MEDIA_TYPE)
public class MultiRecipientMessageProvider implements MessageBodyReader<MultiRecipientMessage> {

  public static final String MEDIA_TYPE = "application/vnd.signal-messenger.mrm";
  public static final int MAX_RECIPIENT_COUNT = 5000;
  public static final int MAX_MESSAGE_SIZE = Math.toIntExact(32 + DataSizeUnit.KIBIBYTES.toBytes(256));
  public static final byte VERSION = 0x22;

  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return MEDIA_TYPE.equals(mediaType.toString()) && MultiRecipientMessage.class.isAssignableFrom(type);
  }

  @Override
  public MultiRecipientMessage readFrom(Class<MultiRecipientMessage> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
      throws IOException, WebApplicationException {
    int versionByte = entityStream.read();
    if (versionByte == -1) {
      throw new NoContentException("Empty body not allowed");
    }
    if (versionByte != VERSION) {
      throw new BadRequestException("Unsupported version");
    }
    long count = readVarint(entityStream);
    if (count > MAX_RECIPIENT_COUNT) {
      throw new BadRequestException("Maximum recipient count exceeded");
    }
    MultiRecipientMessage.Recipient[] recipients = new MultiRecipientMessage.Recipient[Math.toIntExact(count)];
    for (int i = 0; i < Math.toIntExact(count); i++) {
      UUID uuid = readUuid(entityStream);
      long deviceId = readVarint(entityStream);
      byte[] perRecipientKeyMaterial = entityStream.readNBytes(48);
      if (perRecipientKeyMaterial.length != 48) {
        throw new IOException("Failed to read expected number of key material bytes for a recipient");
      }
      recipients[i] = new MultiRecipientMessage.Recipient(uuid, deviceId, perRecipientKeyMaterial);
    }

    // caller is responsible for checking that the entity stream is at EOF when we return; if there are more bytes than
    // this it'll return an error back. We just need to limit how many we'll accept here.
    byte[] commonPayload = entityStream.readNBytes(MAX_MESSAGE_SIZE);
    if (commonPayload.length < 32) {
      throw new IOException("Failed to read expected number of common key material bytes");
    }
    return new MultiRecipientMessage(recipients, commonPayload);
  }

  /**
   * Reads a UUID in network byte order and converts to a UUID object.
   */
  private UUID readUuid(InputStream stream) throws IOException {
    byte[] buffer = new byte[8];

    int read = stream.read(buffer);
    if (read != 8) {
      throw new IOException("Insufficient bytes for UUID");
    }
    long msb = convertNetworkByteOrderToLong(buffer);

    read = stream.read(buffer);
    if (read != 8) {
      throw new IOException("Insufficient bytes for UUID");
    }
    long lsb = convertNetworkByteOrderToLong(buffer);

    return new UUID(msb, lsb);
  }

  private long convertNetworkByteOrderToLong(byte[] buffer) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (buffer[i] & 0xFFL);
    }
    return result;
  }

  /**
   * Reads a varint. A varint larger than 64 bits is rejected with a {@code WebApplicationException}. An
   * {@code IOException} is thrown if the stream ends before we finish reading the varint.
   *
   * @return the varint value
   */
  private long readVarint(InputStream stream) throws IOException, WebApplicationException {
    boolean hasMore = true;
    int currentOffset = 0;
    int result = 0;
    while (hasMore) {
      if (currentOffset >= 64) {
        throw new BadRequestException("varint is too large");
      }
      int b = stream.read();
      if (b == -1) {
        throw new IOException("Missing byte " + (currentOffset / 7) + " of varint");
      }
      if (currentOffset == 63 && (b & 0xFE) != 0) {
        throw new BadRequestException("varint is too large");
      }
      hasMore = (b & 0x80) != 0;
      result |= (b & 0x7F) << currentOffset;
      currentOffset += 7;
    }
    return result;
  }
}
