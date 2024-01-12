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
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;

@Provider
@Consumes(MultiRecipientMessageProvider.MEDIA_TYPE)
public class MultiRecipientMessageProvider implements MessageBodyReader<SealedSenderMultiRecipientMessage> {

  public static final String MEDIA_TYPE = "application/vnd.signal-messenger.mrm";
  public static final int MAX_RECIPIENT_COUNT = 5000;
  public static final int MAX_MESSAGE_SIZE = Math.toIntExact(32 + DataSizeUnit.KIBIBYTES.toBytes(256));

  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return MEDIA_TYPE.equals(mediaType.toString()) && SealedSenderMultiRecipientMessage.class.isAssignableFrom(type);
  }

  @Override
  public SealedSenderMultiRecipientMessage readFrom(Class<SealedSenderMultiRecipientMessage> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
      throws IOException, WebApplicationException {
    byte[] fullMessage = entityStream.readNBytes(MAX_MESSAGE_SIZE + MAX_RECIPIENT_COUNT * 100);
    if (fullMessage.length == 0) {
      throw new NoContentException("Empty body not allowed");
    }

    try {
      final SealedSenderMultiRecipientMessage message = SealedSenderMultiRecipientMessage.parse(fullMessage);
      if (message.getRecipients().values().stream().anyMatch(r -> message.messageSizeForRecipient(r) > MAX_MESSAGE_SIZE)) {
        throw new BadRequestException("message payload too large");
      }
      return message;
    } catch (InvalidMessageException | InvalidVersionException e) {
      throw new BadRequestException(e);
    }
  }
}
