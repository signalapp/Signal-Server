package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.net.InetAddresses;
import io.grpc.Context;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

class RequestAttributesUtilTest {

  private static final InetAddress REMOTE_ADDRESS = InetAddresses.forString("127.0.0.1");

  @Test
  void getAcceptableLanguages() throws Exception {
    assertEquals(Collections.emptyList(),
        callWithRequestAttributes(buildRequestAttributes(Collections.emptyList()),
            RequestAttributesUtil::getAcceptableLanguages));

    assertEquals(Locale.LanguageRange.parse("en,ja"),
        callWithRequestAttributes(buildRequestAttributes(Locale.LanguageRange.parse("en,ja")),
            RequestAttributesUtil::getAcceptableLanguages));
  }

  @Test
  void getAvailableAcceptedLocales() throws Exception {
    assertEquals(Collections.emptyList(),
        callWithRequestAttributes(buildRequestAttributes(Collections.emptyList()),
            RequestAttributesUtil::getAvailableAcceptedLocales));

    final List<Locale> availableAcceptedLocales =
        callWithRequestAttributes(buildRequestAttributes(Locale.LanguageRange.parse("en,ja")),
            RequestAttributesUtil::getAvailableAcceptedLocales);

    assertFalse(availableAcceptedLocales.isEmpty());

    availableAcceptedLocales.forEach(locale ->
        assertTrue("en".equals(locale.getLanguage()) || "ja".equals(locale.getLanguage())));
  }

  @Test
  void getRemoteAddress() throws Exception {
    assertEquals(REMOTE_ADDRESS,
        callWithRequestAttributes(new RequestAttributes(REMOTE_ADDRESS, null, null),
            RequestAttributesUtil::getRemoteAddress));
  }

  @Test
  void getUserAgent() throws Exception {
    assertEquals(Optional.empty(),
        callWithRequestAttributes(buildRequestAttributes((String) null),
            RequestAttributesUtil::getUserAgent));

    assertEquals(Optional.of("Signal-Desktop/1.2.3 Linux"),
        callWithRequestAttributes(buildRequestAttributes("Signal-Desktop/1.2.3 Linux"),
            RequestAttributesUtil::getUserAgent));
  }

  private static <V> V callWithRequestAttributes(final RequestAttributes requestAttributes, final Callable<V> callable) throws Exception {
    return Context.current()
        .withValue(RequestAttributesUtil.REQUEST_ATTRIBUTES_CONTEXT_KEY, requestAttributes)
        .call(callable);
  }

  private static RequestAttributes buildRequestAttributes(final String userAgent) {
    return buildRequestAttributes(userAgent, Collections.emptyList());
  }

  private static RequestAttributes buildRequestAttributes(final List<Locale.LanguageRange> acceptLanguage) {
    return buildRequestAttributes(null, acceptLanguage);
  }

  private static RequestAttributes buildRequestAttributes(@Nullable final String userAgent,
      final List<Locale.LanguageRange> acceptLanguage) {

    return new RequestAttributes(REMOTE_ADDRESS, userAgent, acceptLanguage);
  }
}
