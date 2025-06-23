/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.assertj.core.api.Condition;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerPublicParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ClientZkProfileOperations;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequestContext;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPaymentsConfiguration;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.entities.BaseProfileResponse;
import org.whispersystems.textsecuregcm.entities.BatchIdentityCheckRequest;
import org.whispersystems.textsecuregcm.entities.BatchIdentityCheckResponse;
import org.whispersystems.textsecuregcm.entities.CreateProfileRequest;
import org.whispersystems.textsecuregcm.entities.ExpiringProfileKeyCredentialProfileResponse;
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.entities.VersionedProfileResponse;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.Util;

@ExtendWith(DropwizardExtensionsSupport.class)
class ProfileControllerTest {

  private static final TestClock clock = TestClock.now();
  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final ProfilesManager profilesManager = mock(ProfilesManager.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter rateLimiter = mock(RateLimiter.class);
  private static final RateLimiter usernameRateLimiter = mock(RateLimiter.class);

  private static final PostPolicyGenerator postPolicyGenerator = new PostPolicyGenerator("us-west-1", "profile-bucket",
      "accessKey");
  private static final PolicySigner policySigner = new PolicySigner("accessSecret", "us-west-1");
  private static final ServerZkProfileOperations zkProfileOperations = mock(ServerZkProfileOperations.class);
  private static final ServerSecretParams serverSecretParams = ServerSecretParams.generate();

  private static final byte[] UNIDENTIFIED_ACCESS_KEY = "sixteenbytes1234".getBytes(StandardCharsets.UTF_8);
  private static final IdentityKey ACCOUNT_IDENTITY_KEY = new IdentityKey(Curve.generateKeyPair().getPublicKey());
  private static final IdentityKey ACCOUNT_PHONE_NUMBER_IDENTITY_KEY = new IdentityKey(Curve.generateKeyPair().getPublicKey());
  private static final IdentityKey ACCOUNT_TWO_IDENTITY_KEY = new IdentityKey(Curve.generateKeyPair().getPublicKey());
  private static final IdentityKey ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY = new IdentityKey(Curve.generateKeyPair().getPublicKey());
  private static final String BASE_64_URL_USERNAME_HASH = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final byte[] USERNAME_HASH = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH);
  @SuppressWarnings("unchecked")
  private static final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
      DynamicConfigurationManager.class);

  private DynamicPaymentsConfiguration dynamicPaymentsConfiguration;
  private Account profileAccount;
  private Account capabilitiesAccount;

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ProfileController(
          clock,
          rateLimiters,
          accountsManager,
          profilesManager,
          dynamicConfigurationManager,
          (acceptableLanguages, accountBadges, isSelf) -> List.of(new Badge("TEST", "other", "Test Badge",
              "This badge is in unit tests.", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld")))
          ),
          new BadgesConfiguration(List.of(
              new BadgeConfiguration("TEST", "other", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
              new BadgeConfiguration("TEST1", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
              new BadgeConfiguration("TEST2", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
              new BadgeConfiguration("TEST3", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld")))
          ), List.of("TEST1"), Map.of(1L, "TEST1", 2L, "TEST2", 3L, "TEST3")),
          postPolicyGenerator,
          policySigner,
          serverSecretParams,
          zkProfileOperations,
          Executors.newSingleThreadExecutor()))
      .build();

  @BeforeEach
  void setup() {
    reset(profilesManager);
    clock.pin(Instant.ofEpochSecond(42));
    AccountsHelper.setupMockUpdate(accountsManager);

    dynamicPaymentsConfiguration = mock(DynamicPaymentsConfiguration.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPaymentsConfiguration()).thenReturn(dynamicPaymentsConfiguration);
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes()).thenReturn(Collections.emptyList());

    when(rateLimiters.getProfileLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameLookupLimiter()).thenReturn(usernameRateLimiter);

    profileAccount = mock(Account.class);

    when(profileAccount.getIdentityKey(IdentityType.ACI)).thenReturn(ACCOUNT_TWO_IDENTITY_KEY);
    when(profileAccount.getIdentityKey(IdentityType.PNI)).thenReturn(ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY);
    when(profileAccount.getUuid()).thenReturn(AuthHelper.VALID_UUID_TWO);
    when(profileAccount.getPhoneNumberIdentifier()).thenReturn(AuthHelper.VALID_PNI_TWO);
    when(profileAccount.getCurrentProfileVersion()).thenReturn(Optional.empty());
    when(profileAccount.getUsernameHash()).thenReturn(Optional.of(USERNAME_HASH));
    when(profileAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));
    when(profileAccount.isIdentifiedBy(eq(new AciServiceIdentifier(AuthHelper.VALID_UUID_TWO)))).thenReturn(true);
    when(profileAccount.isIdentifiedBy(eq(new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO)))).thenReturn(true);

    capabilitiesAccount = mock(Account.class);

    when(capabilitiesAccount.getUuid()).thenReturn(AuthHelper.VALID_UUID);
    when(capabilitiesAccount.getIdentityKey(IdentityType.ACI)).thenReturn(ACCOUNT_IDENTITY_KEY);
    when(capabilitiesAccount.getIdentityKey(IdentityType.PNI)).thenReturn(ACCOUNT_PHONE_NUMBER_IDENTITY_KEY);

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());

    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID_TWO)).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByPhoneNumberIdentifier(AuthHelper.VALID_PNI_TWO)).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(AuthHelper.VALID_UUID_TWO))).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByServiceIdentifier(new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO))).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByUsernameHash(USERNAME_HASH)).thenReturn(CompletableFuture.completedFuture(Optional.of(profileAccount)));

    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER)).thenReturn(Optional.of(capabilitiesAccount));
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(capabilitiesAccount));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(AuthHelper.VALID_UUID))).thenReturn(Optional.of(capabilitiesAccount));

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID_TWO)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT_TWO));

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);

    when(profilesManager.get(eq(AuthHelper.VALID_UUID), eq(versionHex("someversion")))).thenReturn(Optional.empty());
    when(profilesManager.get(eq(AuthHelper.VALID_UUID_TWO), eq(versionHex("validversion")))).thenReturn(Optional.of(new VersionedProfile(
        versionHex("validversion"), name, "profiles/validavatar", emoji, about, null, phoneNumberSharing, "validcommitment".getBytes())));

    when(profilesManager.deleteAvatar(anyString())).thenReturn(CompletableFuture.completedFuture(null));

    clearInvocations(rateLimiter);
    clearInvocations(accountsManager);
    clearInvocations(usernameRateLimiter);
    clearInvocations(profilesManager);
    clearInvocations(zkProfileOperations);
  }

  @AfterEach
  void teardown() {
    reset(accountsManager);
    reset(rateLimiter);
  }

  @Test
  void testProfileGetByAci() throws RateLimitExceededException {
    final BaseProfileResponse profile = resources.getJerseyTest()
                              .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
                              .request()
                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                              .get(BaseProfileResponse.class);

    assertThat(profile.getIdentityKey()).isEqualTo(ACCOUNT_TWO_IDENTITY_KEY);
    assertThat(profile.getBadges()).hasSize(1).element(0).has(new Condition<>(
        badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));

    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testProfileGetByAciRateLimited() throws RateLimitExceededException {
    doThrow(new RateLimitExceededException(Duration.ofSeconds(13))).when(rateLimiter)
        .validate(AuthHelper.VALID_UUID);

    final Response response = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    assertThat(response.getStatus()).isEqualTo(429);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(Duration.ofSeconds(13).toSeconds()));
  }

  @Test
  void testProfileGetByAciUnidentified() throws RateLimitExceededException {
    final BaseProfileResponse profile = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader(UNIDENTIFIED_ACCESS_KEY))
        .get(BaseProfileResponse.class);

    assertThat(profile.getIdentityKey()).isEqualTo(ACCOUNT_TWO_IDENTITY_KEY);
    assertThat(profile.getBadges()).hasSize(1).element(0).has(new Condition<>(
        badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));

    verify(rateLimiter, never()).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testProfileGetByAciUnidentifiedBadKey() {
    final Response response = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("incorrect".getBytes()))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testProfileGetByAciUnidentifiedAccountNotFound() {
    final Response response = resources.getJerseyTest()
        .target("/v1/profile/" + UUID.randomUUID())
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader(UNIDENTIFIED_ACCESS_KEY))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @ParameterizedTest
  @MethodSource
  void testProfileGetWithGroupSendEndorsement(
      UUID target, UUID authorizedTarget, Duration timeLeft, boolean includeUak, int expectedResponse) throws Exception {

    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    clock.pin(expiration.minus(timeLeft));

    Invocation.Builder builder = resources.getJerseyTest()
        .target("/v1/profile/" + target)
        .queryParam("pq", "true")
        .request()
        .header(
            HeaderUtils.GROUP_SEND_TOKEN,
            AuthHelper.validGroupSendTokenHeader(serverSecretParams, List.of(new AciServiceIdentifier(authorizedTarget)), expiration));

    if (includeUak) {
      builder = builder.header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader(UNIDENTIFIED_ACCESS_KEY));
    }

    Response response = builder.get();
    assertThat(response.getStatus()).isEqualTo(expectedResponse);

    if (expectedResponse == 200) {
      final BaseProfileResponse profile = response.readEntity(BaseProfileResponse.class);
      assertThat(profile.getIdentityKey()).isEqualTo(ACCOUNT_TWO_IDENTITY_KEY);
      assertThat(profile.getBadges()).hasSize(1).element(0).has(new Condition<>(
              badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));
    }

    verifyNoMoreInteractions(rateLimiter);
  }

  private static Stream<Arguments> testProfileGetWithGroupSendEndorsement() {
    UUID notExistsUuid = UUID.randomUUID();

    return Stream.of(
        // valid endorsement
        Arguments.of(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_UUID_TWO, Duration.ofHours(1), false, 200),

        // expired endorsement, not authorized
        Arguments.of(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_UUID_TWO, Duration.ofHours(-1), false, 401),

        // endorsement for the wrong recipient, not authorized
        Arguments.of(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_UUID, Duration.ofHours(1), false, 401),

        // expired endorsement for the wrong recipient, not authorized
        Arguments.of(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_UUID, Duration.ofHours(-1), false, 401),

        // valid endorsement for the right recipient but they aren't registered, not found
        Arguments.of(notExistsUuid, notExistsUuid, Duration.ofHours(1), false, 404),

        // expired endorsement for the right recipient but they aren't registered, not authorized (NOT not found)
        Arguments.of(notExistsUuid, notExistsUuid, Duration.ofHours(-1), false, 401),

        // valid endorsement but also a UAK, bad request
        Arguments.of(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_UUID_TWO, Duration.ofHours(1), true, 400));
  }

  @Test
  void testProfileGetByPni() throws RateLimitExceededException {
    final BaseProfileResponse profile = resources.getJerseyTest()
        .target("/v1/profile/PNI:" + AuthHelper.VALID_PNI_TWO)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(BaseProfileResponse.class);

    assertThat(profile.getIdentityKey()).isEqualTo(ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY);
    assertThat(profile.getBadges()).isEmpty();
    assertThat(profile.getUuid()).isEqualTo(new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO));
    assertThat(profile.getCapabilities()).isNotNull();
    assertThat(profile.isUnrestrictedUnidentifiedAccess()).isFalse();
    assertThat(profile.getUnidentifiedAccess()).isNull();

    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testProfileGetByPniRateLimited() throws RateLimitExceededException {
    doThrow(new RateLimitExceededException(Duration.ofSeconds(13))).when(rateLimiter)
        .validate(AuthHelper.VALID_UUID);

    final Response response = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_PNI_TWO)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    assertThat(response.getStatus()).isEqualTo(429);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(Duration.ofSeconds(13).toSeconds()));
  }

  @Test
  void testProfileGetByPniUnidentified() throws RateLimitExceededException {
    final Response response = resources.getJerseyTest()
        .target("/v1/profile/PNI:" + AuthHelper.VALID_PNI_TWO)
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader(UNIDENTIFIED_ACCESS_KEY))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);

    verify(rateLimiter, never()).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testProfileGetByPniUnidentifiedBadKey() {
    final Response response = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_PNI_TWO)
        .request()
        .header(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, AuthHelper.getUnidentifiedAccessHeader("incorrect".getBytes()))
        .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testProfileGetUnauthorized() {
    final Response response = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @CartesianTest
  void testProfileCapabilities(
      @CartesianTest.Values(booleans = {true, false}) final boolean isDeleteSyncSupported,
      @CartesianTest.Values(booleans = {true, false}) final boolean isAttachmentBackfillSupported) {
    when(capabilitiesAccount.hasCapability(DeviceCapability.DELETE_SYNC)).thenReturn(isDeleteSyncSupported);
    when(capabilitiesAccount.hasCapability(DeviceCapability.ATTACHMENT_BACKFILL)).thenReturn(isAttachmentBackfillSupported);
    final BaseProfileResponse profile = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(BaseProfileResponse.class);

    assertEquals(isDeleteSyncSupported, profile.getCapabilities().get("deleteSync"));
    assertEquals(isAttachmentBackfillSupported, profile.getCapabilities().get("attachmentBackfill"));
  }

  @Test
  void testSetProfileWantAvatarUpload() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(81);

    final ProfileAvatarUploadAttributes uploadAttributes = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("someversion"),
            name, null, null,
            null, true, false, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE), ProfileAvatarUploadAttributes.class);

    final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID), eq(versionHex("someversion")));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID), profileArgumentCaptor.capture());

    verifyNoMoreInteractions(profilesManager);

    assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().avatar()).isEqualTo(uploadAttributes.getKey());
    assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(versionHex("someversion"));
    assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
    assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().about()).isNull();
  }

  @Test
  void testSetProfileWantAvatarUploadWithBadProfileSize() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(82);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("someversion"), name,
            null, null, null, true, false, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  @Test
  void testSetProfileWithoutAvatarUpload() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("anotherversion"), name, null, null,
            null, false, false, Optional.of(List.of()), phoneNumberSharing), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(versionHex("anotherversion")));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

      verifyNoMoreInteractions(profilesManager);

      assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
      assertThat(profileArgumentCaptor.getValue().avatar()).isNull();
      assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(versionHex("anotherversion"));
      assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
      assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
      assertThat(profileArgumentCaptor.getValue().about()).isNull();
      assertThat(profileArgumentCaptor.getValue().phoneNumberSharing()).isEqualTo(phoneNumberSharing);
    }
  }

  @Test
  void testSetProfileWithAvatarUploadAndPreviousAvatar() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO));
    final byte[] name = TestRandomUtil.nextBytes(81);

    resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("validversion"),
            name, null, null,
            null, true, false, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE), ProfileAvatarUploadAttributes.class);

    final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(versionHex("validversion")));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
    verify(profilesManager, times(1)).deleteAvatar("profiles/validavatar");

    assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().avatar()).startsWith("profiles/");
    assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(versionHex("validversion"));
    assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
    assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().about()).isNull();
  }

  @Test
  void testSetProfileClearPreviousAvatar() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO));
    final byte[] name = TestRandomUtil.nextBytes(81);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("validversion"), name,
            null, null, null, false, false, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(versionHex("validversion")));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
      verify(profilesManager, times(1)).deleteAvatar(eq("profiles/validavatar"));

      assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
      assertThat(profileArgumentCaptor.getValue().avatar()).isNull();
      assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(versionHex("validversion"));
      assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
      assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
      assertThat(profileArgumentCaptor.getValue().about()).isNull();
    }
  }

  @Test
  void testSetProfileWithSameAvatar() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO));
    final byte[] name = TestRandomUtil.nextBytes(81);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("validversion"), name,
            null, null, null, true, true, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(versionHex("validversion")));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
      verify(profilesManager, never()).deleteAvatar(anyString());

      assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
      assertThat(profileArgumentCaptor.getValue().avatar()).isEqualTo("profiles/validavatar");
      assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(versionHex("validversion"));
      assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
      assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
      assertThat(profileArgumentCaptor.getValue().about()).isNull();
    }
  }

  @Test
  void testSetProfileClearPreviousAvatarDespiteSameAvatarFlagSet() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO));
    final byte[] name = TestRandomUtil.nextBytes(81);

    try (final Response ignored = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, versionHex("validversion"), name,
            null, null,
            null, false, true, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(versionHex("validversion")));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
      verify(profilesManager, times(1)).deleteAvatar(eq("profiles/validavatar"));

      assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
      assertThat(profileArgumentCaptor.getValue().avatar()).isNull();
      assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(versionHex("validversion"));
      assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
      assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
      assertThat(profileArgumentCaptor.getValue().about()).isNull();
    }
  }

  @Test
  void testSetProfileWithSameAvatarDespiteNoPreviousAvatar() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(81);
    final String version = versionHex("validversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name,
            null, null, null, true, true, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID), eq(version));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID), profileArgumentCaptor.capture());
      verify(profilesManager, never()).deleteAvatar(anyString());

      assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
      assertThat(profileArgumentCaptor.getValue().avatar()).isNull();
      assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(version);
      assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
      assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
      assertThat(profileArgumentCaptor.getValue().about()).isNull();
    }
  }

  @Test
  void testSetProfileExtendedName() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO));

    final byte[] name = TestRandomUtil.nextBytes(285);

    final String version = versionHex("validversion");
    resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(
            new CreateProfileRequest(commitment, version, name,
                null, null, null, true, false, Optional.of(List.of()), null),
            MediaType.APPLICATION_JSON_TYPE), ProfileAvatarUploadAttributes.class);

    final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(version));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
    verify(profilesManager, times(1)).deleteAvatar("profiles/validavatar");

    assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().avatar()).startsWith("profiles/");
    assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(version);
    assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
    assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().about()).isNull();
  }

  @Test
  void testSetProfileEmojiAndBioText() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);

    final String version = versionHex("anotherversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(
            new CreateProfileRequest(commitment, version, name, emoji, about, null,
                false, false, Optional.of(List.of()), null),
            MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(version));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

      verifyNoMoreInteractions(profilesManager);

      final VersionedProfile profile = profileArgumentCaptor.getValue();
      assertThat(profile.commitment()).isEqualTo(commitment.serialize());
      assertThat(profile.avatar()).isNull();
      assertThat(profile.version()).isEqualTo(version);
      assertThat(profile.name()).isEqualTo(name);
      assertThat(profile.aboutEmoji()).isEqualTo(emoji);
      assertThat(profile.about()).isEqualTo(about);
      assertThat(profile.paymentAddress()).isNull();
    }
  }

  @Test
  void testSetProfilePaymentAddress() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);

    final String version = versionHex("yetanotherversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(
            new CreateProfileRequest(commitment, version, name,
                null, null, paymentAddress, false, false,
                Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager).get(eq(AuthHelper.VALID_UUID_TWO), eq(version));
      verify(profilesManager).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

      verifyNoMoreInteractions(profilesManager);

      final VersionedProfile profile = profileArgumentCaptor.getValue();
      assertThat(profile.commitment()).isEqualTo(commitment.serialize());
      assertThat(profile.avatar()).isNull();
      assertThat(profile.version()).isEqualTo(version);
      assertThat(profile.name()).isEqualTo(name);
      assertThat(profile.aboutEmoji()).isNull();
      assertThat(profile.about()).isNull();
      assertThat(profile.paymentAddress()).isEqualTo(paymentAddress);
    }
  }

  @Test
  void testSetProfilePaymentAddressCountryNotAllowed() throws InvalidInputException {
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes())
        .thenReturn(List.of(AuthHelper.VALID_NUMBER_TWO.substring(0, 3)));

    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(
            new CreateProfileRequest(commitment, versionHex("yetanotherversion"), name,
                null, null, paymentAddress, false, false,
                Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(403);
      assertThat(response.hasEntity()).isFalse();

      verify(profilesManager, never()).set(any(), any());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSetProfilePaymentAddressCountryNotAllowedExistingPaymentAddress(
      final boolean existingPaymentAddressOnProfile) throws InvalidInputException {
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes())
        .thenReturn(List.of(AuthHelper.VALID_NUMBER_TWO.substring(0, 3)));

    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    when(profilesManager.get(eq(AuthHelper.VALID_UUID_TWO), any()))
        .thenReturn(Optional.of(
            new VersionedProfile("1", name, null, null, null,
                existingPaymentAddressOnProfile ? TestRandomUtil.nextBytes(582) : null,
                phoneNumberSharing,
                commitment.serialize())));

    final String version = versionHex("yetanotherversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(
            new CreateProfileRequest(commitment, version, name,
                null, null, paymentAddress, false, false,
                Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      if (existingPaymentAddressOnProfile) {
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.hasEntity()).isFalse();

        final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

        verify(profilesManager).get(eq(AuthHelper.VALID_UUID_TWO), eq(version));
        verify(profilesManager).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

        verifyNoMoreInteractions(profilesManager);

        final VersionedProfile profile = profileArgumentCaptor.getValue();
        assertThat(profile.commitment()).isEqualTo(commitment.serialize());
        assertThat(profile.avatar()).isNull();
        assertThat(profile.version()).isEqualTo(version);
        assertThat(profile.name()).isEqualTo(name);
        assertThat(profile.aboutEmoji()).isNull();
        assertThat(profile.about()).isNull();
        assertThat(profile.paymentAddress()).isEqualTo(paymentAddress);
      } else {
        assertThat(response.getStatus()).isEqualTo(403);
        assertThat(response.hasEntity()).isFalse();

        verify(profilesManager, never()).set(any(), any());
      }
    }
  }

  @Test
  void testSetProfilePhoneNumberSharing() throws Exception {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String version = versionHex("anotherversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name, null, null,
            null, false, false, Optional.of(List.of()), phoneNumberSharing), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

      verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq(version));
      verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

      verifyNoMoreInteractions(profilesManager);

      assertThat(profileArgumentCaptor.getValue().commitment()).isEqualTo(commitment.serialize());
      assertThat(profileArgumentCaptor.getValue().avatar()).isNull();
      assertThat(profileArgumentCaptor.getValue().version()).isEqualTo(version);
      assertThat(profileArgumentCaptor.getValue().name()).isEqualTo(name);
      assertThat(profileArgumentCaptor.getValue().aboutEmoji()).isNull();
      assertThat(profileArgumentCaptor.getValue().about()).isNull();
    }
  }

  @Test
  void testGetProfileByVersion() throws RateLimitExceededException {
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);

    final String version = versionHex("validversion");
    when(profilesManager.get(eq(AuthHelper.VALID_UUID_TWO), eq(version))).thenReturn(Optional.of(new VersionedProfile(
        version, name, "profiles/validavatar", emoji, about, null, phoneNumberSharing, "validcommitment".getBytes())));

    final VersionedProfileResponse profile = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/" + version)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(VersionedProfileResponse.class);

    assertThat(profile.getBaseProfileResponse().getIdentityKey()).isEqualTo(ACCOUNT_TWO_IDENTITY_KEY);
    assertThat(profile.getName()).containsExactly(name);
    assertThat(profile.getAbout()).containsExactly(about);
    assertThat(profile.getAboutEmoji()).containsExactly(emoji);
    assertThat(profile.getAvatar()).isEqualTo("profiles/validavatar");
    assertThat(profile.getPhoneNumberSharing()).containsExactly(phoneNumberSharing);
    assertThat(profile.getBaseProfileResponse().getUuid()).isEqualTo(new AciServiceIdentifier(AuthHelper.VALID_UUID_TWO));
    assertThat(profile.getBaseProfileResponse().getBadges()).hasSize(1).element(0).has(new Condition<>(
        badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));

    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testSetProfileUpdatesAccountCurrentVersion() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);

    final String version = versionHex("someversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(
            new CreateProfileRequest(commitment, version, name, null, null, paymentAddress, false, false,
                Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      verify(AuthHelper.VALID_ACCOUNT_TWO).setCurrentProfileVersion(version);
    }
  }

  @Test
  void testGetProfileReturnsNoPaymentAddressIfCurrentVersionMismatch() {
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final String version = versionHex("validversion");
    when(profilesManager.get(AuthHelper.VALID_UUID_TWO, version)).thenReturn(
        Optional.of(new VersionedProfile(null, null, null, null, null, paymentAddress, null, null)));

    {
      final VersionedProfileResponse profile = resources.getJerseyTest()
          .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/" + version)
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .get(VersionedProfileResponse.class);

      assertThat(profile.getPaymentAddress()).containsExactly(paymentAddress);
    }

    when(profileAccount.getCurrentProfileVersion()).thenReturn(Optional.of(version));

    {
      final VersionedProfileResponse profile = resources.getJerseyTest()
          .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/" + version)
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .get(VersionedProfileResponse.class);

      assertThat(profile.getPaymentAddress()).containsExactly(paymentAddress);
    }

    when(profileAccount.getCurrentProfileVersion()).thenReturn(Optional.of(versionHex("someotherversion")));

    {
      final VersionedProfileResponse profile = resources.getJerseyTest()
          .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/" + version)
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .get(VersionedProfileResponse.class);

      assertThat(profile.getPaymentAddress()).isNull();
    }
  }

  @Test
  void testGetProfileWithExpiringProfileKeyCredentialVersionNotFound() throws VerificationFailedException {
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AuthHelper.VALID_UUID);
    when(account.getCurrentProfileVersion()).thenReturn(Optional.of(versionHex("version")));

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(Optional.empty());

    final ExpiringProfileKeyCredentialProfileResponse profile = resources.getJerseyTest()
        .target(String.format("/v1/profile/%s/%s/%s", AuthHelper.VALID_UUID, versionHex("version-that-does-not-exist"), "credential-request"))
        .queryParam("credentialType", "expiringProfileKey")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(ExpiringProfileKeyCredentialProfileResponse.class);

    assertThat(profile.getVersionedProfileResponse().getBaseProfileResponse().getUuid())
        .isEqualTo(new AciServiceIdentifier(AuthHelper.VALID_UUID));

    assertThat(profile.getCredential()).isNull();

    verify(zkProfileOperations, never()).issueExpiringProfileKeyCredential(any(), any(), any(), any());
  }

  @Test
  void testSetProfileBadges() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);

    final String version = versionHex("anotherversion");
    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name, emoji, about, null, false, false,
            Optional.of(List.of("TEST2")), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      @SuppressWarnings("unchecked")
      final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
      verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

      final List<AccountBadge> badges = badgeCaptor.getValue();
      assertThat(badges).isNotNull().hasSize(1).containsOnly(new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true));

      clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
      when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true)
      ));
    }

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name, emoji, about, null, false, false,
            Optional.of(List.of("TEST3", "TEST2")), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      //noinspection unchecked
      final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
      verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

      final List<AccountBadge> badges = badgeCaptor.getValue();
      assertThat(badges).isNotNull().hasSize(2).containsOnly(
          new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true),
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true));

      clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
      when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
          new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true),
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true)
      ));
    }

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name, emoji, about, null, false, false,
            Optional.of(List.of("TEST2", "TEST3")), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      //noinspection unchecked
      final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
      verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

      final List<AccountBadge> badges = badgeCaptor.getValue();
      assertThat(badges).isNotNull().hasSize(2).containsOnly(
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true),
          new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true));

      clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
      when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true),
          new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true)
      ));
    }

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name, emoji, about, null, false, false,
            Optional.of(List.of("TEST1")), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      //noinspection unchecked
      final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
      verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

      final List<AccountBadge> badges = badgeCaptor.getValue();
      assertThat(badges).isNotNull().hasSize(3).containsOnly(
          new AccountBadge("TEST1", Instant.ofEpochSecond(42 + 86400), true),
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), false),
          new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), false));
    }

  }

  @Test
  void testSetProfileBadgeAfterUpdateTries() throws Exception {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(
        new ServiceId.Aci(AuthHelper.VALID_UUID));

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final String version = versionHex("anotherversion");

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
    reset(accountsManager);
    final int accountsManagerUpdateRetryCount = 2;
    AccountsHelper.setupMockUpdateWithRetries(accountsManager, accountsManagerUpdateRetryCount);
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID_TWO)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT_TWO));
    // set up two invocations -- one for each AccountsManager#update try
    when(AuthHelper.VALID_ACCOUNT_TWO.getBadges())
        .thenReturn(List.of(
            new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true),
            new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true)
        ))
        .thenReturn(List.of(
            new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true),
            new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true),
            new AccountBadge("TEST4", Instant.ofEpochSecond(43 + 86400), true)
        ));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, version, name, emoji, about, null, false, false,
            Optional.of(List.of("TEST1")), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      //noinspection unchecked
      final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
      verify(AuthHelper.VALID_ACCOUNT_TWO, times(accountsManagerUpdateRetryCount)).setBadges(refEq(clock), badgeCaptor.capture());
      // since the stubbing of getBadges() is brittle, we need to verify the number of invocations, to protect against upstream changes
      verify(AuthHelper.VALID_ACCOUNT_TWO, times(accountsManagerUpdateRetryCount)).getBadges();

      final List<AccountBadge> badges = badgeCaptor.getValue();
      assertThat(badges).isNotNull().hasSize(4).containsOnly(
          new AccountBadge("TEST1", Instant.ofEpochSecond(42 + 86400), true),
          new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), false),
          new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), false),
          new AccountBadge("TEST4", Instant.ofEpochSecond(43 + 86400), false));
    }
  }

  @ParameterizedTest
  @MethodSource
  void testGetProfileWithExpiringProfileKeyCredential(final MultivaluedMap<String, Object> authHeaders)
      throws VerificationFailedException, InvalidInputException {
    final String version = versionHex("version");

    final ServerSecretParams serverSecretParams = ServerSecretParams.generate();
    final ServerPublicParams serverPublicParams = serverSecretParams.getPublicParams();

    final ServerZkProfileOperations serverZkProfile = new ServerZkProfileOperations(serverSecretParams);
    final ClientZkProfileOperations clientZkProfile = new ClientZkProfileOperations(serverPublicParams);

    final byte[] profileKeyBytes = TestRandomUtil.nextBytes(32);

    final ProfileKey profileKey = new ProfileKey(profileKeyBytes);
    final ProfileKeyCommitment profileKeyCommitment = profileKey.getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    final VersionedProfile versionedProfile = mock(VersionedProfile.class);
    when(versionedProfile.commitment()).thenReturn(profileKeyCommitment.serialize());

    final ProfileKeyCredentialRequestContext profileKeyCredentialRequestContext =
        clientZkProfile.createProfileKeyCredentialRequestContext(new ServiceId.Aci(AuthHelper.VALID_UUID), profileKey);

    final ProfileKeyCredentialRequest credentialRequest = profileKeyCredentialRequestContext.getRequest();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AuthHelper.VALID_UUID);
    when(account.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));
    when(account.isIdentifiedBy(new AciServiceIdentifier(AuthHelper.VALID_UUID))).thenReturn(true);

    final Instant expiration = Instant.now().plus(org.whispersystems.textsecuregcm.util.ProfileHelper.EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION)
        .truncatedTo(ChronoUnit.DAYS);

    final ExpiringProfileKeyCredentialResponse credentialResponse =
        serverZkProfile.issueExpiringProfileKeyCredential(credentialRequest, new ServiceId.Aci(AuthHelper.VALID_UUID), profileKeyCommitment, expiration);

    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(AuthHelper.VALID_UUID))).thenReturn(Optional.of(account));
    when(profilesManager.get(AuthHelper.VALID_UUID, version)).thenReturn(Optional.of(versionedProfile));
    when(zkProfileOperations.issueExpiringProfileKeyCredential(eq(credentialRequest), eq(new ServiceId.Aci(AuthHelper.VALID_UUID)), eq(profileKeyCommitment), any()))
        .thenReturn(credentialResponse);

    final ExpiringProfileKeyCredentialProfileResponse profile = resources.getJerseyTest()
        .target(String.format("/v1/profile/%s/%s/%s", AuthHelper.VALID_UUID, version,
            HexFormat.of().formatHex(credentialRequest.serialize())))
        .queryParam("credentialType", "expiringProfileKey")
        .request()
        .headers(authHeaders)
        .get(ExpiringProfileKeyCredentialProfileResponse.class);

    assertThat(profile.getVersionedProfileResponse().getBaseProfileResponse().getUuid())
        .isEqualTo(new AciServiceIdentifier(AuthHelper.VALID_UUID));
    assertThat(profile.getCredential()).isEqualTo(credentialResponse);

    verify(zkProfileOperations).issueExpiringProfileKeyCredential(credentialRequest, new ServiceId.Aci(AuthHelper.VALID_UUID), profileKeyCommitment, expiration);

    final ClientZkProfileOperations clientZkProfileCipher = new ClientZkProfileOperations(serverPublicParams);
    assertThatNoException().isThrownBy(() ->
        clientZkProfileCipher.receiveExpiringProfileKeyCredential(profileKeyCredentialRequestContext, profile.getCredential()));
  }

  private static Stream<Arguments> testGetProfileWithExpiringProfileKeyCredential() {
    return Stream.of(
        Arguments.of(new MultivaluedHashMap<>(Map.of(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_KEY)))),
        Arguments.of(new MultivaluedHashMap<>(Map.of("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD)))),
        Arguments.of(new MultivaluedHashMap<>(Map.of("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))))
    );
  }

  @Test
  void testGetProfileWithExpiringProfileKeyCredentialBadRequest()
      throws VerificationFailedException, InvalidInputException {
    final String version = versionHex("version");

    final ServerSecretParams serverSecretParams = ServerSecretParams.generate();
    final ServerPublicParams serverPublicParams = serverSecretParams.getPublicParams();

    final ClientZkProfileOperations clientZkProfile = new ClientZkProfileOperations(serverPublicParams);

    final byte[] profileKeyBytes = TestRandomUtil.nextBytes(32);

    final ProfileKey profileKey = new ProfileKey(profileKeyBytes);
    final ProfileKeyCommitment profileKeyCommitment = profileKey.getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    final VersionedProfile versionedProfile = mock(VersionedProfile.class);
    when(versionedProfile.commitment()).thenReturn(profileKeyCommitment.serialize());

    final ProfileKeyCredentialRequestContext profileKeyCredentialRequestContext =
        clientZkProfile.createProfileKeyCredentialRequestContext(new ServiceId.Aci(AuthHelper.VALID_UUID), profileKey);

    final ProfileKeyCredentialRequest credentialRequest = profileKeyCredentialRequestContext.getRequest();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AuthHelper.VALID_UUID);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));
    when(account.isIdentifiedBy(new AciServiceIdentifier(AuthHelper.VALID_UUID))).thenReturn(true);

    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(AuthHelper.VALID_UUID))).thenReturn(Optional.of(account));
    when(profilesManager.get(AuthHelper.VALID_UUID, version)).thenReturn(Optional.of(versionedProfile));
    when(zkProfileOperations.issueExpiringProfileKeyCredential(any(), any(), any(), any()))
        .thenThrow(new VerificationFailedException());

    final Response response = resources.getJerseyTest()
        .target(String.format("/v1/profile/%s/%s/%s", AuthHelper.VALID_UUID, version,
            HexFormat.of().formatHex(credentialRequest.serialize())))
        .queryParam("credentialType", "expiringProfileKey")
        .request()
        .headers(new MultivaluedHashMap<>(Map.of(HeaderUtils.UNIDENTIFIED_ACCESS_KEY, Base64.getEncoder().encodeToString(UNIDENTIFIED_ACCESS_KEY))))
        .get();

    assertEquals(400, response.getStatus());
  }

  @Test
  void testSetProfileBadgesMissingFromRequest() throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = ProfileTestHelper.generateRandomBase64FromByteArray(81);
    final String emoji = ProfileTestHelper.generateRandomBase64FromByteArray(60);
    final String text = ProfileTestHelper.generateRandomBase64FromByteArray(156);

    when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
        new AccountBadge("TEST", Instant.ofEpochSecond(42 + 86400), true)
    ));

    // Older clients may not include badges in their requests
    final String requestJson = String.format("""
        {
          "commitment": "%s",
          "version": "%s",
          "name": "%s",
          "avatar": false,
          "aboutEmoji": "%s",
          "about": "%s"
        }
        """,
        Base64.getEncoder().encodeToString(commitment.serialize()), versionHex("version"), name, emoji, text);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.json(requestJson))) {

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.hasEntity()).isFalse();

      verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), eq(List.of(new AccountBadge("TEST", Instant.ofEpochSecond(42 + 86400), true))));
    }
  }

  @Test
  void testBatchIdentityCheck() {
    try (final Response response = resources.getJerseyTest().target("/v1/profile/identity_check/batch").request()
        .post(Entity.json(new BatchIdentityCheckRequest(List.of(
            new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(AuthHelper.VALID_UUID),
                convertKeyToFingerprint(ACCOUNT_IDENTITY_KEY)),
            new BatchIdentityCheckRequest.Element(new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO),
                convertKeyToFingerprint(ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY)),
            new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(AuthHelper.INVALID_UUID),
                convertKeyToFingerprint(ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY))
        ))))) {
      assertThat(response).isNotNull();
      assertThat(response.getStatus()).isEqualTo(200);
      BatchIdentityCheckResponse identityCheckResponse = response.readEntity(BatchIdentityCheckResponse.class);
      assertThat(identityCheckResponse).isNotNull();
      assertThat(identityCheckResponse.elements()).isNotNull().isEmpty();
    }

    final Map<ServiceIdentifier, IdentityKey> expectedIdentityKeys = Map.of(
        new AciServiceIdentifier(AuthHelper.VALID_UUID), ACCOUNT_IDENTITY_KEY,
        new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO), ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY,
        new AciServiceIdentifier(AuthHelper.VALID_UUID_TWO), ACCOUNT_TWO_IDENTITY_KEY);

    final Condition<BatchIdentityCheckResponse.Element> isAnExpectedUuid =
        new Condition<>(element -> element.identityKey()
            .equals(expectedIdentityKeys.get(element.uuid())),
            "is an expected UUID with the correct identity key");

    final IdentityKey validAciIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());
    final IdentityKey secondValidPniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());
    final IdentityKey invalidAciIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());

    try (final Response response = resources.getJerseyTest().target("/v1/profile/identity_check/batch").request()
        .post(Entity.json(new BatchIdentityCheckRequest(List.of(
            new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(AuthHelper.VALID_UUID),
                convertKeyToFingerprint(validAciIdentityKey)),
            new BatchIdentityCheckRequest.Element(new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO),
                convertKeyToFingerprint(secondValidPniIdentityKey)),
            new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(AuthHelper.INVALID_UUID),
                convertKeyToFingerprint(invalidAciIdentityKey))
        ))))) {
      assertThat(response).isNotNull();
      assertThat(response.getStatus()).isEqualTo(200);
      BatchIdentityCheckResponse identityCheckResponse = response.readEntity(BatchIdentityCheckResponse.class);
      assertThat(identityCheckResponse).isNotNull();
      assertThat(identityCheckResponse.elements()).isNotNull().hasSize(2);
      assertThat(identityCheckResponse.elements()).element(0).isNotNull().is(isAnExpectedUuid);
      assertThat(identityCheckResponse.elements()).element(1).isNotNull().is(isAnExpectedUuid);
    }

    final List<BatchIdentityCheckRequest.Element> largeElementList = new ArrayList<>(List.of(
        new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(AuthHelper.VALID_UUID),
            convertKeyToFingerprint(validAciIdentityKey)),
        new BatchIdentityCheckRequest.Element(new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO),
            convertKeyToFingerprint(secondValidPniIdentityKey)),
        new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(AuthHelper.INVALID_UUID),
            convertKeyToFingerprint(invalidAciIdentityKey))));

    for (int i = 0; i < 900; i++) {
      largeElementList.add(
          new BatchIdentityCheckRequest.Element(new AciServiceIdentifier(UUID.randomUUID()),
              convertKeyToFingerprint(new IdentityKey(Curve.generateKeyPair().getPublicKey()))));
    }

    try (final Response response = resources.getJerseyTest().target("/v1/profile/identity_check/batch").request()
        .post(Entity.json(new BatchIdentityCheckRequest(largeElementList)))) {
      assertThat(response).isNotNull();
      assertThat(response.getStatus()).isEqualTo(200);
      BatchIdentityCheckResponse identityCheckResponse = response.readEntity(BatchIdentityCheckResponse.class);
      assertThat(identityCheckResponse).isNotNull();
      assertThat(identityCheckResponse.elements()).isNotNull().hasSize(2);
      assertThat(identityCheckResponse.elements()).element(0).isNotNull().is(isAnExpectedUuid);
      assertThat(identityCheckResponse.elements()).element(1).isNotNull().is(isAnExpectedUuid);
    }
  }

  @Test
  void testBatchIdentityCheckDeserialization() throws Exception {

    final Map<ServiceIdentifier, IdentityKey> expectedIdentityKeys = Map.of(
        new AciServiceIdentifier(AuthHelper.VALID_UUID), ACCOUNT_IDENTITY_KEY,
        new PniServiceIdentifier(AuthHelper.VALID_PNI_TWO), ACCOUNT_TWO_PHONE_NUMBER_IDENTITY_KEY);

    final Condition<BatchIdentityCheckResponse.Element> isAnExpectedUuid =
        new Condition<>(element -> element.identityKey().equals(expectedIdentityKeys.get(element.uuid())),
            "is an expected UUID with the correct identity key");

    // null properties are ok to omit
    final String json = String.format("""
            {
              "elements": [
                { "uuid": "%s", "fingerprint": "%s" },
                { "uuid": "%s", "fingerprint": "%s" },
                { "uuid": "%s", "fingerprint": "%s" }
              ]
            }
            """, AuthHelper.VALID_UUID, Base64.getEncoder().encodeToString(convertKeyToFingerprint(new IdentityKey(Curve.generateKeyPair().getPublicKey()))),
        "PNI:" + AuthHelper.VALID_PNI_TWO, Base64.getEncoder().encodeToString(convertKeyToFingerprint(new IdentityKey(Curve.generateKeyPair().getPublicKey()))),
        AuthHelper.INVALID_UUID, Base64.getEncoder().encodeToString(convertKeyToFingerprint(new IdentityKey(Curve.generateKeyPair().getPublicKey()))));

    try (final Response response = resources.getJerseyTest().target("/v1/profile/identity_check/batch").request()
        .post(Entity.entity(json, "application/json"))) {
      assertThat(response).isNotNull();
      assertThat(response.getStatus()).isEqualTo(200);
      String responseJson = response.readEntity(String.class);

      // `null` properties should be omitted from the response
      assertThat(responseJson).doesNotContain("null");

      final BatchIdentityCheckResponse identityCheckResponse =
          SystemMapper.jsonMapper().readValue(responseJson, BatchIdentityCheckResponse.class);

      assertThat(identityCheckResponse).isNotNull();
      assertThat(identityCheckResponse.elements()).isNotNull().hasSize(2);
      assertThat(identityCheckResponse.elements()).element(0).isNotNull().is(isAnExpectedUuid);
      assertThat(identityCheckResponse.elements()).element(1).isNotNull().is(isAnExpectedUuid);
    }
  }

  @ParameterizedTest
  @MethodSource
  void testBatchIdentityCheckDeserializationBadRequest(final String json, final int expectedStatus) {
    try (final Response response = resources.getJerseyTest().target("/v1/profile/identity_check/batch").request()
        .post(Entity.entity(json, "application/json"))) {
      assertThat(response).isNotNull();
      assertThat(response.getStatus()).isEqualTo(expectedStatus);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "64charactersbutnothexFFFFFFFFFFF64charactersbutnothexFFFFFFFFFFF", "DEADBEEF"})
  void testInvalidVersionString(final String version) throws InvalidInputException {
    final ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(
        new ServiceId.Aci(AuthHelper.VALID_UUID));
    final byte[] name = TestRandomUtil.nextBytes(81);

    try (final Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new CreateProfileRequest(commitment, version,
                name, null, null,
                null, true, false, Optional.of(List.of()), null), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  static Stream<Arguments> testBatchIdentityCheckDeserializationBadRequest() {
    return Stream.of(
        Arguments.of( // aci and uuid cannot both be null
            String.format("""
                {
                  "elements": [
                    { "uuid": null, "fingerprint": "%s" }
                  ]
                }
                """, Base64.getEncoder().encodeToString(convertKeyToFingerprint(new IdentityKey(Curve.generateKeyPair().getPublicKey())))),
            422),
        Arguments.of( // a blank string is invalid
            String.format("""
                {
                  "elements": [
                    { "uuid": " ", "fingerprint": "%s" }
                  ]
                }
                """, Base64.getEncoder().encodeToString(convertKeyToFingerprint(new IdentityKey(Curve.generateKeyPair().getPublicKey())))),
            400)
    );
  }



  private static byte[] convertKeyToFingerprint(final IdentityKey publicKey) {
    try {
      return Util.truncate(MessageDigest.getInstance("SHA-256").digest(publicKey.serialize()), 4);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("All Java implementations must support SHA-256 MessageDigest algorithm", e);
    }
  }

  private static String versionHex(final String versionString) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(versionString.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }
}
