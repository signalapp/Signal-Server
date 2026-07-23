/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.signal.chat.common.Badge;
import org.signal.chat.remoteconfiguration.Badges;
import org.signal.chat.remoteconfiguration.Configuration;
import org.signal.chat.remoteconfiguration.GetBadgesRequest;
import org.signal.chat.remoteconfiguration.GetBadgesResponse;
import org.signal.chat.remoteconfiguration.GetConfigurationRequest;
import org.signal.chat.remoteconfiguration.GetConfigurationResponse;
import org.signal.chat.remoteconfiguration.SimpleRemoteConfigurationGrpc;
import org.signal.chat.remoteconfiguration.TaggedBadges;
import org.signal.chat.remoteconfiguration.TaggedConfiguration;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.badges.BadgeTranslator;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;

public class RemoteConfigurationGrpcService extends SimpleRemoteConfigurationGrpc.RemoteConfigurationImplBase {

  private static final GetConfigurationResponse CONFIGURATION_ETAG_MATCHED =
      GetConfigurationResponse.newBuilder().setEtagMatched(true).build();
  private static final GetBadgesResponse BADGES_ETAG_MATCHED =
      GetBadgesResponse.newBuilder().setEtagMatched(true).build();

  private final RemoteConfigsManager remoteConfigsManager;
  private final BadgeTranslator badgeTranslator;
  private final List<String> badgeIds;

  // Badge information varies based on the provided Accept-Language header. Here we cache the etag for the resolved
  // language, so if the caller provides a matching etag we don't have to build the full badge response.
  private final ConcurrentHashMap<Locale, ByteString> localeToBadgesEtag = new ConcurrentHashMap<>();

  public RemoteConfigurationGrpcService(
      final RemoteConfigsManager remoteConfigsManager,
      final BadgeTranslator badgeTranslator,
      final List<String> badgeIds) {
    this.remoteConfigsManager = remoteConfigsManager;
    this.badgeTranslator = badgeTranslator;
    this.badgeIds = badgeIds;
  }

  @Override
  public GetConfigurationResponse getConfiguration(final GetConfigurationRequest request) {
    final UUID accountIdentifier = AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier();
    final String userAgent = RequestAttributesUtil.getUserAgent().orElse(null);
    final Map<String, String> configForAccount = remoteConfigsManager.getConfigForAccount(accountIdentifier, userAgent);

    final Configuration configuration = Configuration.newBuilder()
        .putAllConfiguration(configForAccount)
        .build();

    final ByteString etagByteString = etag(configuration);
    if (etagByteString.equals(request.getEtag())) {
      return CONFIGURATION_ETAG_MATCHED;
    }
    return GetConfigurationResponse.newBuilder()
        .setTaggedConfiguration(TaggedConfiguration.newBuilder()
            .setEtag(etagByteString)
            .setConfiguration(configuration))
        .build();
  }

  @Override
  public GetBadgesResponse getBadges(final GetBadgesRequest request) {
    final List<Locale> acceptableLanguages = RequestAttributesUtil.getAvailableAcceptedLocales();
    final Locale locale = badgeTranslator.resolveLocale(acceptableLanguages);

    final ByteString cachedEtag = localeToBadgesEtag.get(locale);
    if (cachedEtag != null && cachedEtag.equals(request.getEtag())) {
      return BADGES_ETAG_MATCHED;
    }

    final Map<String, Badge> badgesById = badgeIds.stream().collect(Collectors.toMap(
        Function.identity(),
        badgeId -> BadgeGrpcHelper.toGrpcBadge(badgeTranslator.translate(acceptableLanguages, badgeId))));
    final Badges badges = Badges.newBuilder().putAllBadges(badgesById).build();

    final TaggedBadges taggedBadges = TaggedBadges.newBuilder()
        .setBadges(badges)
        .setEtag(etag(badges))
        .build();

    // This could race and multiple threads could decide to build-and-cache. That's fine, they should all calculate
    // the same etag.
    localeToBadgesEtag.put(locale, taggedBadges.getEtag());
    return GetBadgesResponse.newBuilder().setTaggedBadges(taggedBadges).build();
  }

  private static ByteString etag(final Message message) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(message.getSerializedSize());
    final CodedOutputStream cos = CodedOutputStream.newInstance(baos);
    cos.useDeterministicSerialization();
    try {
      message.writeTo(cos);
      cos.flush();
      return ByteString.copyFrom(MessageDigest.getInstance("SHA-256").digest(baos.toByteArray()));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }
}
