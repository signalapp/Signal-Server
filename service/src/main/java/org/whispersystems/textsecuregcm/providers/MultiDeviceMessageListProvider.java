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
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NoContentException;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.whispersystems.textsecuregcm.entities.IncomingDeviceMessage;

@Provider
@Consumes(MultiDeviceMessageListProvider.MEDIA_TYPE)
public class MultiDeviceMessageListProvider extends BinaryProviderBase implements MessageBodyReader<IncomingDeviceMessage[]> {

  public static final String MEDIA_TYPE = "application/vnd.signal-messenger.mdml";
  public static final int MAX_MESSAGE_COUNT = 50;
  public static final int MAX_MESSAGE_SIZE = Math.toIntExact(DataSizeUnit.KIBIBYTES.toBytes(256));
  public static final byte VERSION = 0x01;

  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return MEDIA_TYPE.equals(mediaType.toString()) && IncomingDeviceMessage[].class.isAssignableFrom(type);
  }

  @Override
  public IncomingDeviceMessage[]
  readFrom(Class<IncomingDeviceMessage[]> resultType, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream)
      throws IOException, WebApplicationException {
    int versionByte = entityStream.read();
    if (versionByte == -1) {
      throw new NoContentException("Empty body not allowed");
    }
    if (versionByte != VERSION) {
      throw new BadRequestException("Unsupported version");
    }
    int count = entityStream.read();
    if (count == -1) {
      throw new IOException("Missing count");
    }
    if (count > MAX_MESSAGE_COUNT) {
      throw new BadRequestException("Maximum recipient count exceeded");
    }
    IncomingDeviceMessage[] messages = new IncomingDeviceMessage[count];
    for (int i = 0; i < count; i++) {
      long deviceId = readVarint(entityStream);
      int registrationId = readU16(entityStream);

      int type = entityStream.read();
      if (type == -1) {
        throw new IOException("Unexpected end of stream reading message type");
      }

      long messageLength = readVarint(entityStream);
      if (messageLength > MAX_MESSAGE_SIZE) {
        throw new WebApplicationException("Message body too large", Status.REQUEST_ENTITY_TOO_LARGE);
      }
      byte[] contents = entityStream.readNBytes(Math.toIntExact(messageLength));
      if (contents.length != messageLength) {
        throw new IOException("Unexpected end of stream in the middle of message contents");
      }

      messages[i] = new IncomingDeviceMessage(type, deviceId, registrationId, contents);
    }
    return messages;
  }
}
