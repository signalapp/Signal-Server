/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class AcceptLanguageInterceptor implements ServerInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(AcceptLanguageInterceptor.class);
  private static final String INVALID_ACCEPT_LANGUAGE_COUNTER_NAME = name(AcceptLanguageInterceptor.class, "invalidAcceptLanguage");

  @VisibleForTesting
  public static final Metadata.Key<String> ACCEPTABLE_LANGUAGES_GRPC_HEADER =
      Metadata.Key.of("accept-language", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    final List<Locale> locales = parseLocales(headers.get(ACCEPTABLE_LANGUAGES_GRPC_HEADER));

    return Contexts.interceptCall(
        Context.current().withValue(AcceptLanguageUtil.ACCEPTABLE_LANGUAGES_CONTEXT_KEY, locales),
        call,
        headers,
        next);
  }

  static List<Locale> parseLocales(@Nullable final String acceptableLanguagesHeader) {
    if (acceptableLanguagesHeader == null) {
      return Collections.emptyList();
    }
    try {
      final List<Locale.LanguageRange> languageRanges = Locale.LanguageRange.parse(acceptableLanguagesHeader);
      return Locale.filter(languageRanges, Arrays.asList(Locale.getAvailableLocales()));
    } catch (final IllegalArgumentException e) {
      final UserAgent userAgent = UserAgentUtil.userAgentFromGrpcContext();
      Metrics.counter(INVALID_ACCEPT_LANGUAGE_COUNTER_NAME, "platform", userAgent.getPlatform().name().toLowerCase()).increment();
      logger.debug("Could not get acceptable languages; Accept-Language: {}; User-Agent: {}",
          acceptableLanguagesHeader,
          userAgent,
          e);
      return Collections.emptyList();
    }
  }
}

