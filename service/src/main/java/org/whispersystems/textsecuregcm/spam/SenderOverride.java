/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;

/**
 * A SenderOverride may be provided by an upstream request filter. If request contains a property for
 * {@link #SMS_SENDER_OVERRIDE_PROPERTY_NAME} or {@link #VOICE_SENDER_OVERRIDE_PROPERTY_NAME} it can be
 * forwarded to a downstream filter to indicate a specific sender should be used when sending verification codes.
 */
public class SenderOverride {

  private static final Logger logger = LoggerFactory.getLogger(SenderOverride.class);
  public static final String SMS_SENDER_OVERRIDE_PROPERTY_NAME = "smsSenderOverride";
  public static final String VOICE_SENDER_OVERRIDE_PROPERTY_NAME = "voiceSenderOverride";

  /**
   * The name of the sender to use to deliver a verification code via SMS
   */
  private final Optional<String> smsSenderOverride;

  /**
   * The name of the sender to use to deliver a verification code via voice
   */
  private final Optional<String> voiceSenderOverride;

  public SenderOverride(final ContainerRequest containerRequest) {
    this.smsSenderOverride = parse(String.class, SMS_SENDER_OVERRIDE_PROPERTY_NAME, containerRequest);
    this.voiceSenderOverride = parse(String.class, VOICE_SENDER_OVERRIDE_PROPERTY_NAME, containerRequest);
  }

  private static <T> Optional<T> parse(Class<T> type, final String propertyName,
      final ContainerRequest containerRequest) {
    return Optional
        .ofNullable(containerRequest.getProperty(propertyName))
        .flatMap(obj -> {
          if (type.isInstance(obj)) {
            return Optional.of(type.cast(obj));
          }
          logger.warn("invalid format for filter provided property {}: {}", propertyName, obj);
          return Optional.empty();
        });
  }


  public Optional<String> getSmsSenderOverride() {
    return smsSenderOverride;
  }

  public Optional<String> getVoiceSenderOverride() {
    return voiceSenderOverride;
  }

}
