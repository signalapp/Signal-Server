/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Condition;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKey;
import org.signal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPaymentsConfiguration;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.entities.CreateProfileRequest;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

@ExtendWith(DropwizardExtensionsSupport.class)
class ProfileControllerTest {

  private static final Clock clock = mock(Clock.class);
  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final ProfilesManager profilesManager = mock(ProfilesManager.class);
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);
  private static final RateLimiter rateLimiter = mock(RateLimiter.class);
  private static final RateLimiter usernameRateLimiter = mock(RateLimiter.class);

  private static final S3Client s3client = mock(S3Client.class);
  private static final PostPolicyGenerator postPolicyGenerator = new PostPolicyGenerator("us-west-1", "profile-bucket", "accessKey");
  private static final PolicySigner policySigner = new PolicySigner("accessSecret", "us-west-1");
  private static final ServerZkProfileOperations zkProfileOperations = mock(ServerZkProfileOperations.class);

  @SuppressWarnings("unchecked")
  private static final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

  private DynamicPaymentsConfiguration dynamicPaymentsConfiguration;
  private Account profileAccount;

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.getMapper())
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
          s3client,
          postPolicyGenerator,
          policySigner,
          "profilesBucket",
          zkProfileOperations))
      .build();

  @BeforeEach
  void setup() {
    reset(s3client);

    when(clock.instant()).thenReturn(Instant.ofEpochSecond(42));

    AccountsHelper.setupMockUpdate(accountsManager);

    dynamicPaymentsConfiguration = mock(DynamicPaymentsConfiguration.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPaymentsConfiguration()).thenReturn(dynamicPaymentsConfiguration);
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes()).thenReturn(Collections.emptyList());

    when(rateLimiters.getProfileLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameLookupLimiter()).thenReturn(usernameRateLimiter);

    profileAccount = mock(Account.class);

    when(profileAccount.getIdentityKey()).thenReturn("bar");
    when(profileAccount.getProfileName()).thenReturn("baz");
    when(profileAccount.getAvatar()).thenReturn("profiles/bang");
    when(profileAccount.getUuid()).thenReturn(AuthHelper.VALID_UUID_TWO);
    when(profileAccount.isEnabled()).thenReturn(true);
    when(profileAccount.isGroupsV2Supported()).thenReturn(false);
    when(profileAccount.isGv1MigrationSupported()).thenReturn(false);
    when(profileAccount.isSenderKeySupported()).thenReturn(false);
    when(profileAccount.isAnnouncementGroupSupported()).thenReturn(false);
    when(profileAccount.isChangeNumberSupported()).thenReturn(false);
    when(profileAccount.getCurrentProfileVersion()).thenReturn(Optional.empty());
    when(profileAccount.getUsername()).thenReturn(Optional.of("n00bkiller"));

    Account capabilitiesAccount = mock(Account.class);

    when(capabilitiesAccount.getIdentityKey()).thenReturn("barz");
    when(capabilitiesAccount.getProfileName()).thenReturn("bazz");
    when(capabilitiesAccount.getAvatar()).thenReturn("profiles/bangz");
    when(capabilitiesAccount.isEnabled()).thenReturn(true);
    when(capabilitiesAccount.isGroupsV2Supported()).thenReturn(true);
    when(capabilitiesAccount.isGv1MigrationSupported()).thenReturn(true);
    when(capabilitiesAccount.isSenderKeySupported()).thenReturn(true);
    when(capabilitiesAccount.isAnnouncementGroupSupported()).thenReturn(true);
    when(capabilitiesAccount.isChangeNumberSupported()).thenReturn(true);

    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID_TWO)).thenReturn(Optional.of(profileAccount));
    when(accountsManager.getByUsername("n00bkiller")).thenReturn(Optional.of(profileAccount));

    when(accountsManager.getByE164(AuthHelper.VALID_NUMBER)).thenReturn(Optional.of(capabilitiesAccount));
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(capabilitiesAccount));

    when(profilesManager.get(eq(AuthHelper.VALID_UUID), eq("someversion"))).thenReturn(Optional.empty());
    when(profilesManager.get(eq(AuthHelper.VALID_UUID_TWO), eq("validversion"))).thenReturn(Optional.of(new VersionedProfile(
        "validversion", "validname", "profiles/validavatar", "emoji", "about", null, "validcommitmnet".getBytes())));

    clearInvocations(rateLimiter);
    clearInvocations(accountsManager);
    clearInvocations(usernameRateLimiter);
    clearInvocations(profilesManager);
  }

  @AfterEach
  void teardown() {
    reset(accountsManager);
  }

  @Test
  void testProfileGetByUuid() throws RateLimitExceededException {
    Profile profile= resources.getJerseyTest()
                              .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
                              .request()
                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                              .get(Profile.class);

    assertThat(profile.getIdentityKey()).isEqualTo("bar");
    assertThat(profile.getName()).isEqualTo("baz");
    assertThat(profile.getAvatar()).isEqualTo("profiles/bang");
    assertThat(profile.getUsername()).isEqualTo("n00bkiller");
    assertThat(profile.getBadges()).hasSize(1).element(0).has(new Condition<>(
        badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));

    verify(accountsManager).getByAccountIdentifier(AuthHelper.VALID_UUID_TWO);
    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testProfileGetByUsername() throws RateLimitExceededException {
    Profile profile= resources.getJerseyTest()
                              .target("/v1/profile/username/n00bkiller")
                              .request()
                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                              .get(Profile.class);

    assertThat(profile.getIdentityKey()).isEqualTo("bar");
    assertThat(profile.getName()).isEqualTo("baz");
    assertThat(profile.getAvatar()).isEqualTo("profiles/bang");
    assertThat(profile.getUsername()).isEqualTo("n00bkiller");
    assertThat(profile.getUuid()).isEqualTo(AuthHelper.VALID_UUID_TWO);
    assertThat(profile.getBadges()).hasSize(1).element(0).has(new Condition<>(
        badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));

    verify(accountsManager).getByUsername("n00bkiller");
    verify(usernameRateLimiter, times(1)).validate(eq(AuthHelper.VALID_UUID));
  }

  @Test
  void testProfileGetUnauthorized() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
                                 .request()
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testProfileGetByUsernameUnauthorized() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/username/n00bkiller")
                                 .request()
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }


  @Test
  void testProfileGetByUsernameNotFound() throws RateLimitExceededException {
    Response response = resources.getJerseyTest()
                              .target("/v1/profile/username/n00bkillerzzzzz")
                              .request()
                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                              .get();

    assertThat(response.getStatus()).isEqualTo(404);

    verify(accountsManager).getByUsername("n00bkillerzzzzz");
    verify(usernameRateLimiter).validate(eq(AuthHelper.VALID_UUID));
  }


  @Test
  void testProfileGetDisabled() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testProfileCapabilities() {
    Profile profile= resources.getJerseyTest()
                              .target("/v1/profile/" + AuthHelper.VALID_UUID)
                              .request()
                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                              .get(Profile.class);

    assertThat(profile.getCapabilities().isGv2()).isTrue();
    assertThat(profile.getCapabilities().isGv1Migration()).isTrue();
    assertThat(profile.getCapabilities().isSenderKey()).isTrue();
    assertThat(profile.getCapabilities().isAnnouncementGroup()).isTrue();

    profile = resources
        .getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .get(Profile.class);

    assertThat(profile.getCapabilities().isGv2()).isFalse();
    assertThat(profile.getCapabilities().isGv1Migration()).isFalse();
    assertThat(profile.getCapabilities().isSenderKey()).isFalse();
    assertThat(profile.getCapabilities().isAnnouncementGroup()).isFalse();
  }

  @Test
  void testSetProfileNameDeprecated() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/name/123456789012345678901234567890123456789012345678901234567890123456789012")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager, times(1)).update(any(Account.class), any());
  }

  @Test
  void testSetProfileNameExtendedDeprecated() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/name/123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager, times(1)).update(any(Account.class), any());
  }

  @Test
  void testSetProfileNameWrongSizeDeprecated() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/name/1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(400);
    verifyNoMoreInteractions(accountsManager);
  }

  /////

  @Test
  void testSetProfileWantAvatarUpload() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    ProfileAvatarUploadAttributes uploadAttributes = resources.getJerseyTest()
                                                              .target("/v1/profile/")
                                                              .request()
                                                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                                              .put(Entity.entity(new CreateProfileRequest(commitment, "someversion", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678", null, null,
                                                                  null, true, List.of()), MediaType.APPLICATION_JSON_TYPE), ProfileAvatarUploadAttributes.class);

    ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID), eq("someversion"));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID), profileArgumentCaptor.capture());

    verifyNoMoreInteractions(s3client);

    assertThat(profileArgumentCaptor.getValue().getCommitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().getAvatar()).isEqualTo(uploadAttributes.getKey());
    assertThat(profileArgumentCaptor.getValue().getVersion()).isEqualTo("someversion");
    assertThat(profileArgumentCaptor.getValue().getName()).isEqualTo("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678");
    assertThat(profileArgumentCaptor.getValue().getAboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().getAbout()).isNull();  }

  @Test
  void testSetProfileWantAvatarUploadWithBadProfileSize() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                 .put(Entity.entity(new CreateProfileRequest(commitment, "someversion", "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", null, null,
                                     null, true, List.of()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testSetProfileWithoutAvatarUpload() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    Response response = resources.getJerseyTest()
                                 .target("/v1/profile/")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
                                 .put(Entity.entity(new CreateProfileRequest(commitment, "anotherversion", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678", null, null,
                                     null, false, List.of()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq("anotherversion"));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

    verify(AuthHelper.VALID_ACCOUNT_TWO).setProfileName("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678");
    verify(AuthHelper.VALID_ACCOUNT_TWO).setAvatar(null);

    verifyNoMoreInteractions(s3client);

    assertThat(profileArgumentCaptor.getValue().getCommitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().getAvatar()).isNull();
    assertThat(profileArgumentCaptor.getValue().getVersion()).isEqualTo("anotherversion");
    assertThat(profileArgumentCaptor.getValue().getName()).isEqualTo("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678");
    assertThat(profileArgumentCaptor.getValue().getAboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().getAbout()).isNull();
  }

  @Test
  void testSetProfileWithAvatarUploadAndPreviousAvatar() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID_TWO);

    resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "validversion",
            "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678",
            null, null,
            null, true, List.of()), MediaType.APPLICATION_JSON_TYPE), ProfileAvatarUploadAttributes.class);

    ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq("validversion"));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
    verify(s3client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket("profilesBucket").key("profiles/validavatar").build()));

    assertThat(profileArgumentCaptor.getValue().getCommitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().getAvatar()).startsWith("profiles/");
    assertThat(profileArgumentCaptor.getValue().getVersion()).isEqualTo("validversion");
    assertThat(profileArgumentCaptor.getValue().getName()).isEqualTo("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678");
    assertThat(profileArgumentCaptor.getValue().getAboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().getAbout()).isNull();  }

  @Test
  void testSetProfileExtendedName() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);

    resources.getJerseyTest()
            .target("/v1/profile/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
            .put(Entity.entity(new CreateProfileRequest(commitment, "validversion", name, null, null, null, true, List.of()), MediaType.APPLICATION_JSON_TYPE), ProfileAvatarUploadAttributes.class);

    ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq("validversion"));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());
    verify(s3client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket("profilesBucket").key("profiles/validavatar").build()));

    assertThat(profileArgumentCaptor.getValue().getCommitment()).isEqualTo(commitment.serialize());
    assertThat(profileArgumentCaptor.getValue().getAvatar()).startsWith("profiles/");
    assertThat(profileArgumentCaptor.getValue().getVersion()).isEqualTo("validversion");
    assertThat(profileArgumentCaptor.getValue().getName()).isEqualTo(name);
    assertThat(profileArgumentCaptor.getValue().getAboutEmoji()).isNull();
    assertThat(profileArgumentCaptor.getValue().getAbout()).isNull();
  }

  @Test
  void testSetProfileEmojiAndBioText() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);
    final String emoji = RandomStringUtils.randomAlphanumeric(80);
    final String text = RandomStringUtils.randomAlphanumeric(720);

    Response response = resources.getJerseyTest()
            .target("/v1/profile/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
            .put(Entity.entity(new CreateProfileRequest(commitment, "anotherversion", name, emoji, text, null, false, List.of()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq("anotherversion"));
    verify(profilesManager, times(1)).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

    verify(AuthHelper.VALID_ACCOUNT_TWO).setProfileName(name);
    verify(AuthHelper.VALID_ACCOUNT_TWO).setAvatar(null);

    verifyNoMoreInteractions(s3client);

    final VersionedProfile profile = profileArgumentCaptor.getValue();
    assertThat(profile.getCommitment()).isEqualTo(commitment.serialize());
    assertThat(profile.getAvatar()).isNull();
    assertThat(profile.getVersion()).isEqualTo("anotherversion");
    assertThat(profile.getName()).isEqualTo(name);
    assertThat(profile.getAboutEmoji()).isEqualTo(emoji);
    assertThat(profile.getAbout()).isEqualTo(text);
    assertThat(profile.getPaymentAddress()).isNull();
  }

  @Test
  void testSetProfilePaymentAddress() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);
    final String paymentAddress = RandomStringUtils.randomAlphanumeric(776);

    Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "yetanotherversion", name, null, null, paymentAddress, false, List.of()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager).get(eq(AuthHelper.VALID_UUID_TWO), eq("yetanotherversion"));
    verify(profilesManager).set(eq(AuthHelper.VALID_UUID_TWO), profileArgumentCaptor.capture());

    verify(AuthHelper.VALID_ACCOUNT_TWO).setProfileName(eq(name));
    verify(AuthHelper.VALID_ACCOUNT_TWO).setAvatar(null);

    verifyNoMoreInteractions(s3client);

    final VersionedProfile profile = profileArgumentCaptor.getValue();
    assertThat(profile.getCommitment()).isEqualTo(commitment.serialize());
    assertThat(profile.getAvatar()).isNull();
    assertThat(profile.getVersion()).isEqualTo("yetanotherversion");
    assertThat(profile.getName()).isEqualTo(name);
    assertThat(profile.getAboutEmoji()).isNull();
    assertThat(profile.getAbout()).isNull();
    assertThat(profile.getPaymentAddress()).isEqualTo(paymentAddress);
  }

  @Test
  void testSetProfilePaymentAddressCountryNotAllowed() throws InvalidInputException {
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes())
        .thenReturn(List.of(AuthHelper.VALID_NUMBER_TWO.substring(0, 3)));

    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);
    final String paymentAddress = RandomStringUtils.randomAlphanumeric(776);

    Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "yetanotherversion", name, null, null, paymentAddress, false, List.of()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
    assertThat(response.hasEntity()).isFalse();

    verify(profilesManager, never()).set(any(), any());
  }

  @Test
  void testGetProfileByVersion() throws RateLimitExceededException {
    Profile profile = resources.getJerseyTest()
                               .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/validversion")
                               .request()
                               .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                               .get(Profile.class);

    assertThat(profile.getIdentityKey()).isEqualTo("bar");
    assertThat(profile.getName()).isEqualTo("validname");
    assertThat(profile.getAbout()).isEqualTo("about");
    assertThat(profile.getAboutEmoji()).isEqualTo("emoji");
    assertThat(profile.getAvatar()).isEqualTo("profiles/validavatar");
    assertThat(profile.getCapabilities().isGv2()).isFalse();
    assertThat(profile.getCapabilities().isGv1Migration()).isFalse();
    assertThat(profile.getUsername()).isEqualTo("n00bkiller");
    assertThat(profile.getUuid()).isNull();
    assertThat(profile.getBadges()).hasSize(1).element(0).has(new Condition<>(
        badge -> "Test Badge".equals(badge.getName()), "has badge with expected name"));

    verify(accountsManager, times(1)).getByAccountIdentifier(eq(AuthHelper.VALID_UUID_TWO));
    verify(profilesManager, times(1)).get(eq(AuthHelper.VALID_UUID_TWO), eq("validversion"));

    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testSetProfileUpdatesAccountCurrentVersion() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID_TWO);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);
    final String paymentAddress = RandomStringUtils.randomAlphanumeric(776);

    Response response = resources.getJerseyTest()
        .target("/v1/profile")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "someversion", name, null, null, paymentAddress, false, List.of()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    verify(AuthHelper.VALID_ACCOUNT_TWO).setCurrentProfileVersion("someversion");
  }

  @Test
  void testGetProfileReturnsNoPaymentAddressIfCurrentVersionMismatch() {
    when(profilesManager.get(AuthHelper.VALID_UUID_TWO, "validversion")).thenReturn(
        Optional.of(new VersionedProfile(null, null, null, null, null, "paymentaddress", null)));
    Profile profile = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/validversion")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(Profile.class);
    assertThat(profile.getPaymentAddress()).isEqualTo("paymentaddress");

    when(profileAccount.getCurrentProfileVersion()).thenReturn(Optional.of("validversion"));
    profile = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/validversion")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(Profile.class);
    assertThat(profile.getPaymentAddress()).isEqualTo("paymentaddress");

    when(profileAccount.getCurrentProfileVersion()).thenReturn(Optional.of("someotherversion"));
    profile = resources.getJerseyTest()
        .target("/v1/profile/" + AuthHelper.VALID_UUID_TWO + "/validversion")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(Profile.class);
    assertThat(profile.getPaymentAddress()).isNull();
  }

  @Test
  void testSetProfileBadges() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);
    final String emoji = RandomStringUtils.randomAlphanumeric(80);
    final String text = RandomStringUtils.randomAlphanumeric(720);

    Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "anotherversion", name, emoji, text, null, false, List.of("TEST2")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
    verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

    List<AccountBadge> badges = badgeCaptor.getValue();
    assertThat(badges).isNotNull().hasSize(1).containsOnly(new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
    when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
        new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true)
    ));

    response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "anotherversion", name, emoji, text, null, false, List.of("TEST3", "TEST2")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    //noinspection unchecked
    badgeCaptor = ArgumentCaptor.forClass(List.class);
    verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

    badges = badgeCaptor.getValue();
    assertThat(badges).isNotNull().hasSize(2).containsOnly(
        new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true),
        new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
    when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
        new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true),
        new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true)
    ));

    response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "anotherversion", name, emoji, text, null, false, List.of("TEST2", "TEST3")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    //noinspection unchecked
    badgeCaptor = ArgumentCaptor.forClass(List.class);
    verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

    badges = badgeCaptor.getValue();
    assertThat(badges).isNotNull().hasSize(2).containsOnly(
        new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true),
        new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true));

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);
    when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
        new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), true),
        new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), true)
    ));

    response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.entity(new CreateProfileRequest(commitment, "anotherversion", name, emoji, text, null, false, List.of("TEST1")), MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    //noinspection unchecked
    badgeCaptor = ArgumentCaptor.forClass(List.class);
    verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), badgeCaptor.capture());

    badges = badgeCaptor.getValue();
    assertThat(badges).isNotNull().hasSize(3).containsOnly(
        new AccountBadge("TEST1", Instant.ofEpochSecond(42 + 86400), true),
        new AccountBadge("TEST2", Instant.ofEpochSecond(42 + 86400), false),
        new AccountBadge("TEST3", Instant.ofEpochSecond(42 + 86400), false));
  }

  @Test
  void testSetProfileBadgesMissingFromRequest() throws InvalidInputException {
    ProfileKeyCommitment commitment = new ProfileKey(new byte[32]).getCommitment(AuthHelper.VALID_UUID);

    clearInvocations(AuthHelper.VALID_ACCOUNT_TWO);

    final String name = RandomStringUtils.randomAlphabetic(380);
    final String emoji = RandomStringUtils.randomAlphanumeric(80);
    final String text = RandomStringUtils.randomAlphanumeric(720);

    when(AuthHelper.VALID_ACCOUNT_TWO.getBadges()).thenReturn(List.of(
        new AccountBadge("TEST", Instant.ofEpochSecond(42 + 86400), true)
    ));

    // Older clients may not include badges in their requests
    final String requestJson = String.format("""
        {
          "commitment": "%s",
          "version": "version",
          "name": "%s",
          "avatar": false,
          "aboutEmoji": "%s",
          "about": "%s"
        }
        """,
        Base64.getEncoder().encodeToString(commitment.serialize()), name, emoji, text);

    Response response = resources.getJerseyTest()
        .target("/v1/profile/")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
        .put(Entity.json(requestJson));

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.hasEntity()).isFalse();

    verify(AuthHelper.VALID_ACCOUNT_TWO).setBadges(refEq(clock), eq(List.of(new AccountBadge("TEST", Instant.ofEpochSecond(42 + 86400), true))));
  }
}
