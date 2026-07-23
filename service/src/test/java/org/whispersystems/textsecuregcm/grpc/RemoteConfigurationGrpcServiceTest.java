/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.remoteconfiguration.GetBadgesRequest;
import org.signal.chat.remoteconfiguration.GetBadgesResponse;
import org.signal.chat.remoteconfiguration.GetConfigurationRequest;
import org.signal.chat.remoteconfiguration.GetConfigurationResponse;
import org.signal.chat.remoteconfiguration.RemoteConfigurationGrpc;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;

class RemoteConfigurationGrpcServiceTest extends
    SimpleBaseGrpcTest<RemoteConfigurationGrpcService, RemoteConfigurationGrpc.RemoteConfigurationBlockingStub> {

  private static final List<String> BADGE_IDS = List.of("B1", "B2");

  @Mock
  private RemoteConfigsManager remoteConfigsManager;

  @Mock
  private BadgeTranslator badgeTranslator;

  @Override
  protected RemoteConfigurationGrpcService createServiceBeforeEachTest() {
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, null));

    when(badgeTranslator.resolveLocale(any())).thenReturn(Locale.US);
    when(badgeTranslator.translate(any(), eq("B1"))).thenReturn(new Badge("B1", "cat1", "name1", "desc1",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(any(), eq("B2"))).thenReturn(new Badge("B2", "cat2", "name2", "desc2",
        List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));

    return new RemoteConfigurationGrpcService(remoteConfigsManager, badgeTranslator, BADGE_IDS);
  }

  @Test
  void getConfigurationUnchanged() {
    final String userAgent = "Signal-Android/7.6.2 Android/34 libsignal/0.46.0";
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), userAgent, null));
    when(remoteConfigsManager.getConfigForAccount(AUTHENTICATED_ACI, userAgent))
        .thenReturn(Map.of("test.test", "bar", "global.test", "false"));

    final GetConfigurationResponse response = authenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().build());

    assertTrue(response.hasTaggedConfiguration());
    final ByteString etag = response.getTaggedConfiguration().getEtag();

    final GetConfigurationResponse cachedResponse = authenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().setEtag(etag).build());

    assertTrue(cachedResponse.getEtagMatched());
    assertFalse(cachedResponse.hasTaggedConfiguration());
  }

  @Test
  void getConfigurationChanged() {
    final String userAgent = "Signal-Android/7.6.2 Android/34 libsignal/0.46.0";
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), userAgent, null));

    final Map<String, String> config1 = Map.of(
        "android.stickers", "foo",
        "test.test", "bar",
        "global.test", "false");

    when(remoteConfigsManager.getConfigForAccount(AUTHENTICATED_ACI, userAgent)).thenReturn(config1);

    final GetConfigurationResponse response = authenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().build());

    assertTrue(response.hasTaggedConfiguration());
    final ByteString etag = response.getTaggedConfiguration().getEtag();

    final Map<String, String> config2 = new HashMap<>(config1);
    config2.put("android.new.config", "true");
    when(remoteConfigsManager.getConfigForAccount(AUTHENTICATED_ACI, userAgent)).thenReturn(config2);

    final GetConfigurationResponse afterChange = authenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().setEtag(etag).build());

    assertFalse(afterChange.getEtagMatched());
    assertTrue(afterChange.hasTaggedConfiguration());
  }

  @ParameterizedTest
  @MethodSource
  void getConfigurationEtag(final boolean expectMatched, final Map<String, String> config1,
      final Map<String, String> config2) {

    final UUID user1 = UUID.randomUUID();
    final String ua1 = "user-agent-1";
    final UUID user2 = UUID.randomUUID();
    final String ua2 = "user-agent-2";

    when(remoteConfigsManager.getConfigForAccount(user1, ua1)).thenReturn(config1);
    when(remoteConfigsManager.getConfigForAccount(user2, ua2)).thenReturn(config2);

    getMockAuthenticationInterceptor().setAuthenticatedDevice(user1, AUTHENTICATED_DEVICE_ID);
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), ua1, null));
    final GetConfigurationResponse response = authenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().build());

    assertTrue(response.hasTaggedConfiguration());
    final ByteString etag = response.getTaggedConfiguration().getEtag();

    getMockAuthenticationInterceptor().setAuthenticatedDevice(user2, AUTHENTICATED_DEVICE_ID);
    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), ua2, null));
    final GetConfigurationResponse secondResponse = authenticatedServiceStub()
        .getConfiguration(GetConfigurationRequest.newBuilder().setEtag(etag).build());

    if (expectMatched) {
      assertTrue(secondResponse.getEtagMatched());
      assertFalse(secondResponse.hasTaggedConfiguration());
    } else {
      assertFalse(secondResponse.getEtagMatched());
      assertTrue(secondResponse.hasTaggedConfiguration());
    }
  }

  static List<Arguments> getConfigurationEtag() {
    final Map<String, String> config1 = Map.of(
        "android.stickers", "foo",
        "test.test", "bar",
        "global.test", "false");

    final Map<String, String> config2 = Map.of(
        "android.stickers", "foo",
        "test.test", "bar",
        "global.test", "true");

    return List.of(
        Arguments.argumentSet("same", true, config1, config1),
        Arguments.argumentSet("change", false, config1, config2),
        Arguments.argumentSet("different sort", true,
            new TreeMap<>(config1),
            config1.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, () -> new TreeMap<String, String>(Comparator.reverseOrder()))))
    );
  }

  @Test
  void getBadges() {
    final GetBadgesResponse response = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().build());

    assertTrue(response.hasTaggedBadges());
    final Map<String, org.signal.chat.common.Badge> badges =
        response.getTaggedBadges().getBadges().getBadgesMap();
    assertEquals(Set.of("B1", "B2"), badges.keySet());
    assertEquals("B1", badges.get("B1").getId());
    assertEquals("cat1", badges.get("B1").getCategory());
    assertEquals("B2", badges.get("B2").getId());
  }

  @Test
  void getBadgesEtagMatched() {
    final GetBadgesResponse response = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().build());
    final ByteString etag = response.getTaggedBadges().getEtag();

    final GetBadgesResponse cachedResponse = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().setEtag(etag).build());

    assertTrue(cachedResponse.getEtagMatched());
    assertFalse(cachedResponse.hasTaggedBadges());
  }

  @Test
  void getBadgesWrongEtag() {
    final GetBadgesResponse response = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().setEtag(ByteString.copyFrom(new byte[32])).build());

    assertFalse(response.hasEtagMatched());
    assertTrue(response.hasTaggedBadges());
  }

  @Test
  void getBadgesComputesOncePerLocale() {
    final RequestAttributes frAttributes =
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "fr-fr");
    final List<Locale> frLocales = List.of(Locale.FRANCE);
    final RequestAttributes caAttributes =
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, "en-ca");
    final List<Locale> caLocales = List.of(Locale.CANADA);

    when(badgeTranslator.resolveLocale(frLocales)).thenReturn(Locale.FRANCE);
    when(badgeTranslator.resolveLocale(caLocales)).thenReturn(Locale.CANADA);

    // return slightly different values based on the language so the locales configurations have different etags
    when(badgeTranslator.translate(eq(frLocales), eq("B1")))
        .thenReturn(new Badge("B1", "cat1", "name1", "desc1", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));
    when(badgeTranslator.translate(eq(caLocales), eq("B1")))
        .thenReturn(new Badge("B1", "dog1", "name1", "desc1", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"))));

    getMockRequestAttributesInterceptor().setRequestAttributes(caAttributes);
    final GetBadgesResponse caResponse = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().build());
    final GetBadgesResponse caResponseCached = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().setEtag(caResponse.getTaggedBadges().getEtag()).build());
    assertTrue(caResponseCached.getEtagMatched());
    verify(badgeTranslator, times(1)).translate(eq(caLocales), eq("B1"));

    getMockRequestAttributesInterceptor().setRequestAttributes(frAttributes);
    final GetBadgesResponse frResponse = authenticatedServiceStub()
        .getBadges(GetBadgesRequest.newBuilder().setEtag(caResponse.getTaggedBadges().getEtag()).build());
    assertFalse(frResponse.hasEtagMatched());
    verify(badgeTranslator, times(1)).translate(eq(frLocales), eq("B1"));
  }
}
