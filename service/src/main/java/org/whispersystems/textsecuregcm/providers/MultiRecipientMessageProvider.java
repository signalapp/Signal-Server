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
public class MultiRecipientMessageProvider extends BinaryProviderBase implements MessageBodyReader<MultiRecipientMessage> {

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
      int registrationId = readU16(entityStream);
      byte[] perRecipientKeyMaterial = entityStream.readNBytes(48);
      if (perRecipientKeyMaterial.length != 48) {
        throw new IOException("Failed to read expected number of key material bytes for a recipient");
      }
      recipients[i] = new MultiRecipientMessage.Recipient(uuid, deviceId, registrationId, perRecipientKeyMaterial);
    }

    // caller is responsible for checking that the entity stream is at EOF when we return; if there are more bytes than
    // this it'll return an error back. We just need to limit how many we'll accept here.
    byte[] commonPayload = entityStream.readNBytes(MAX_MESSAGE_SIZE);
    if (commonPayload.length < 32) {
      throw new IOException("Failed to read expected number of common key material bytes");
    }
    return new MultiRecipientMessage(recipients, commonPayload);
  }
}
