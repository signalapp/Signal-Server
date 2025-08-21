/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.dropwizard.util.DataSizeUnit;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.NoContentException;
import jakarta.ws.rs.ext.MessageBodyReader;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;

@Provider
@Consumes(MultiRecipientMessageProvider.MEDIA_TYPE)
public class MultiRecipientMessageProvider implements MessageBodyReader<SealedSenderMultiRecipientMessage> {

  public static final String MEDIA_TYPE = "application/vnd.signal-messenger.mrm";
  public static final int MAX_RECIPIENT_COUNT = 5000;
  public static final int MAX_MESSAGE_SIZE = Math.toIntExact(32 + DataSizeUnit.KIBIBYTES.toBytes(256));

  private static final DistributionSummary RECIPIENT_COUNT_DISTRIBUTION = DistributionSummary
      .builder(name(MultiRecipientMessageProvider.class, "recipients"))
      .publishPercentileHistogram(true)
      .register(Metrics.globalRegistry);

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
      RECIPIENT_COUNT_DISTRIBUTION.record(message.getRecipients().size());
      return message;
    } catch (InvalidMessageException | InvalidVersionException e) {
      throw new BadRequestException(e);
    }
  }
}
