/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.util.DataSizeUnit;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NoContentException;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.whispersystems.textsecuregcm.entities.MultiRecipientMessage;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

@Provider
@Consumes(MultiRecipientMessageProvider.MEDIA_TYPE)
public class MultiRecipientMessageProvider implements MessageBodyReader<MultiRecipientMessage> {

  public static final String MEDIA_TYPE = "application/vnd.signal-messenger.mrm";
  public static final int MAX_RECIPIENT_COUNT = 5000;
  public static final int MAX_MESSAGE_SIZE = Math.toIntExact(32 + DataSizeUnit.KIBIBYTES.toBytes(256));

  public static final byte AMBIGUOUS_ID_VERSION_IDENTIFIER = 0x22;
  public static final byte EXPLICIT_ID_VERSION_IDENTIFIER = 0x23;

  private enum Version {
    AMBIGUOUS_ID(AMBIGUOUS_ID_VERSION_IDENTIFIER),
    EXPLICIT_ID(EXPLICIT_ID_VERSION_IDENTIFIER);

    private final byte identifier;

    Version(final byte identifier) {
      this.identifier = identifier;
    }

    static Version forVersionByte(final byte versionByte) {
      for (final Version version : values()) {
        if (version.identifier == versionByte) {
          return version;
        }
      }

      throw new IllegalArgumentException("Unrecognized version byte: " + versionByte);
    }
  }

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

    final Version version;

    try {
      version = Version.forVersionByte((byte) versionByte);
    } catch (final IllegalArgumentException e) {
      throw new BadRequestException("Unsupported version");
    }

    long count = readVarint(entityStream);
    if (count > MAX_RECIPIENT_COUNT) {
      throw new BadRequestException("Maximum recipient count exceeded");
    }
    MultiRecipientMessage.Recipient[] recipients = new MultiRecipientMessage.Recipient[Math.toIntExact(count)];
    for (int i = 0; i < Math.toIntExact(count); i++) {
      ServiceIdentifier identifier = readIdentifier(entityStream, version);
      long deviceId = readVarint(entityStream);
      int registrationId = readU16(entityStream);
      byte[] perRecipientKeyMaterial = entityStream.readNBytes(48);
      if (perRecipientKeyMaterial.length != 48) {
        throw new IOException("Failed to read expected number of key material bytes for a recipient");
      }
      recipients[i] = new MultiRecipientMessage.Recipient(identifier, deviceId, registrationId, perRecipientKeyMaterial);
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
   * Reads a service identifier from the given stream.
   */
  private ServiceIdentifier readIdentifier(final InputStream stream, final Version version) throws IOException {
    final byte[] uuidBytes = switch (version) {
      case AMBIGUOUS_ID -> stream.readNBytes(16);
      case EXPLICIT_ID -> stream.readNBytes(17);
    };

    return ServiceIdentifier.fromBytes(uuidBytes);
  }

  /**
   * Reads a varint. A varint larger than 64 bits is rejected with a {@code WebApplicationException}. An
   * {@code IOException} is thrown if the stream ends before we finish reading the varint.
   *
   * @return the varint value
   */
  @VisibleForTesting
  public static long readVarint(InputStream stream) throws IOException, WebApplicationException {
    boolean hasMore = true;
    int currentOffset = 0;
    long result = 0;
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
      result |= (b & 0x7FL) << currentOffset;
      currentOffset += 7;
    }
    return result;
  }

  /**
   * Reads two bytes with most significant byte first. Treats the value as unsigned so the range returned is
   * {@code [0, 65535]}.
   */
  @VisibleForTesting
  static int readU16(InputStream stream) throws IOException {
    int b1 = stream.read();
    if (b1 == -1) {
      throw new IOException("Missing byte 1 of U16");
    }
    int b2 = stream.read();
    if (b2 == -1) {
      throw new IOException("Missing byte 2 of U16");
    }
    return (b1 << 8) | b2;
  }
}
