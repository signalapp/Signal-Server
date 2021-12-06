/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.VerificationFailedException;
import org.signal.zkgroup.profiles.PniCredentialResponse;
import org.signal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.zkgroup.profiles.ProfileKeyCredentialResponse;
import org.signal.zkgroup.profiles.ServerZkProfileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.CreateProfileRequest;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.entities.UserCapabilities;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/profile")
public class ProfileController {

  private final Logger logger = LoggerFactory.getLogger(ProfileController.class);

  private final Clock clock;
  private final RateLimiters     rateLimiters;
  private final ProfilesManager  profilesManager;
  private final AccountsManager  accountsManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final ProfileBadgeConverter profileBadgeConverter;
  private final Map<String, BadgeConfiguration> badgeConfigurationMap;

  private final PolicySigner              policySigner;
  private final PostPolicyGenerator       policyGenerator;
  private final ServerZkProfileOperations zkProfileOperations;

  private final S3Client            s3client;
  private final String              bucket;

  private static final String PROFILE_KEY_CREDENTIAL_TYPE = "profileKey";
  private static final String PNI_CREDENTIAL_TYPE = "pni";

  public ProfileController(
      Clock clock,
      RateLimiters rateLimiters,
      AccountsManager accountsManager,
      ProfilesManager profilesManager,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      ProfileBadgeConverter profileBadgeConverter,
      BadgesConfiguration badgesConfiguration,
      S3Client s3client,
      PostPolicyGenerator policyGenerator,
      PolicySigner policySigner,
      String bucket,
      ServerZkProfileOperations zkProfileOperations) {
    this.clock = clock;
    this.rateLimiters        = rateLimiters;
    this.accountsManager     = accountsManager;
    this.profilesManager     = profilesManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.profileBadgeConverter = profileBadgeConverter;
    this.badgeConfigurationMap = badgesConfiguration.getBadges().stream().collect(Collectors.toMap(
        BadgeConfiguration::getId, Function.identity()));
    this.zkProfileOperations = zkProfileOperations;
    this.bucket              = bucket;
    this.s3client            = s3client;
    this.policyGenerator     = policyGenerator;
    this.policySigner        = policySigner;
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setProfile(@Auth AuthenticatedAccount auth, @Valid CreateProfileRequest request) {
    if (StringUtils.isNotBlank(request.getPaymentAddress())) {
      final boolean hasDisallowedPrefix =
          dynamicConfigurationManager.getConfiguration().getPaymentsConfiguration().getDisallowedPrefixes().stream()
              .anyMatch(prefix -> auth.getAccount().getNumber().startsWith(prefix));

      if (hasDisallowedPrefix) {
        return Response.status(Status.FORBIDDEN).build();
      }
    }

    Optional<VersionedProfile> currentProfile = profilesManager.get(auth.getAccount().getUuid(), request.getVersion());
    String avatar = request.isAvatar() ? generateAvatarObjectName() : null;
    Optional<ProfileAvatarUploadAttributes> response = Optional.empty();

    profilesManager.set(auth.getAccount().getUuid(),
        new VersionedProfile(
            request.getVersion(),
            request.getName(),
            avatar,
            request.getAboutEmoji(),
            request.getAbout(),
            request.getPaymentAddress(),
            request.getCommitment().serialize()));

    if (request.isAvatar()) {
      Optional<String> currentAvatar = Optional.empty();

      if (currentProfile.isPresent() && currentProfile.get().getAvatar() != null && currentProfile.get().getAvatar()
          .startsWith("profiles/")) {
        currentAvatar = Optional.of(currentProfile.get().getAvatar());
      }

      if (currentAvatar.isEmpty() && auth.getAccount().getAvatar() != null && auth.getAccount().getAvatar()
          .startsWith("profiles/")) {
        currentAvatar = Optional.of(auth.getAccount().getAvatar());
      }

      currentAvatar.ifPresent(s -> s3client.deleteObject(DeleteObjectRequest.builder()
          .bucket(bucket)
          .key(s)
          .build()));

      response = Optional.of(generateAvatarUploadForm(avatar));
    }

    List<AccountBadge> updatedBadges = request.getBadges()
        .map(badges -> mergeBadgeIdsWithExistingAccountBadges(badges, auth.getAccount().getBadges()))
        .orElseGet(() -> auth.getAccount().getBadges());

    accountsManager.update(auth.getAccount(), a -> {
      a.setProfileName(request.getName());
      a.setAvatar(avatar);
      a.setBadges(clock, updatedBadges);
      a.setCurrentProfileVersion(request.getVersion());
    });

    if (response.isPresent()) {
      return Response.ok(response).build();
    } else {
      return Response.ok().build();
    }
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{uuid}/{version}")
  public Profile getProfile(
      @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("uuid") UUID uuid,
      @PathParam("version") String version)
      throws RateLimitExceededException {
    return getVersionedProfile(auth.map(AuthenticatedAccount::getAccount), accessKey,
        getAcceptableLanguagesForRequest(containerRequestContext), uuid,
        version, Optional.empty(), Optional.empty());
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{uuid}/{version}/{credentialRequest}")
  public Profile getProfile(
      @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("uuid") UUID uuid,
      @PathParam("version") String version,
      @PathParam("credentialRequest") String credentialRequest,
      @QueryParam("credentialType") @DefaultValue(PROFILE_KEY_CREDENTIAL_TYPE) String credentialType)
      throws RateLimitExceededException {
    return getVersionedProfile(auth.map(AuthenticatedAccount::getAccount), accessKey,
        getAcceptableLanguagesForRequest(containerRequestContext), uuid,
        version, Optional.of(credentialRequest), Optional.of(credentialType));
  }

  private Profile getVersionedProfile(
      Optional<Account> requestAccount,
      Optional<Anonymous> accessKey,
      List<Locale> acceptableLanguages,
      UUID uuid,
      String version,
      Optional<String> credentialRequest,
      Optional<String> credentialType)
      throws RateLimitExceededException {
    if (requestAccount.isEmpty() && accessKey.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    boolean isSelf = false;
    if (requestAccount.isPresent()) {
      UUID authedUuid = requestAccount.get().getUuid();
      rateLimiters.getProfileLimiter().validate(authedUuid);
      isSelf = uuid.equals(authedUuid);
    }

    Optional<Account> accountProfile = accountsManager.getByAccountIdentifier(uuid);
    OptionalAccess.verify(requestAccount, accessKey, accountProfile);

    assert accountProfile.isPresent();

    Optional<String>           username   = accountProfile.flatMap(Account::getUsername);
    Optional<VersionedProfile> profile    = profilesManager.get(uuid, version);

    String name = profile.map(VersionedProfile::getName).orElse(accountProfile.get().getProfileName());
    String about = profile.map(VersionedProfile::getAbout).orElse(null);
    String aboutEmoji = profile.map(VersionedProfile::getAboutEmoji).orElse(null);
    String avatar = profile.map(VersionedProfile::getAvatar).orElse(accountProfile.get().getAvatar());
    Optional<String> currentProfileVersion = accountProfile.get().getCurrentProfileVersion();

    // Allow requests where either the version matches the latest version on Account or the latest version on Account
    // is empty to read the payment address.
    final String paymentAddress = profile
        .filter(p -> currentProfileVersion.map(v -> v.equals(version)).orElse(true))
        .map(VersionedProfile::getPaymentAddress)
        .orElse(null);

    final ProfileKeyCredentialResponse profileKeyCredentialResponse;
    final PniCredentialResponse pniCredentialResponse;

    if (credentialRequest.isPresent() && credentialType.isPresent() && profile.isPresent()) {
      if (PNI_CREDENTIAL_TYPE.equals(credentialType.get())) {
        if (!isSelf) {
          throw new ForbiddenException();
        }

        profileKeyCredentialResponse = null;
        pniCredentialResponse = getPniCredential(credentialRequest.get(),
            profile.get(),
            requestAccount.get().getUuid(),
            requestAccount.get().getPhoneNumberIdentifier());
      } else if (PROFILE_KEY_CREDENTIAL_TYPE.equals(credentialType.get())) {
        profileKeyCredentialResponse = getProfileCredential(credentialRequest.get(),
            profile.get(),
            uuid);

        pniCredentialResponse = null;
      } else {
        throw new BadRequestException();
      }
    } else {
      profileKeyCredentialResponse = null;
      pniCredentialResponse = null;
    }

    return new Profile(
        name,
        about,
        aboutEmoji,
        avatar,
        paymentAddress,
        accountProfile.get().getIdentityKey(),
        UnidentifiedAccessChecksum.generateFor(accountProfile.get().getUnidentifiedAccessKey()),
        accountProfile.get().isUnrestrictedUnidentifiedAccess(),
        UserCapabilities.createForAccount(accountProfile.get()),
        username.orElse(null),
        null,
        profileBadgeConverter.convert(acceptableLanguages, accountProfile.get().getBadges(), isSelf),
        profileKeyCredentialResponse,
        pniCredentialResponse);
  }


  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/username/{username}")
  public Profile getProfileByUsername(
      @Auth AuthenticatedAccount auth,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("username") String username)
      throws RateLimitExceededException {
    rateLimiters.getUsernameLookupLimiter().validate(auth.getAccount().getUuid());

    username = username.toLowerCase();

    final Account accountProfile = accountsManager.getByUsername(username)
        .orElseThrow(() -> new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build()));

    final boolean isSelf = auth.getAccount().getUuid().equals(accountProfile.getUuid());

    return new Profile(
        accountProfile.getProfileName(),
        null,
        null,
        accountProfile.getAvatar(),
        null,
        accountProfile.getIdentityKey(),
        UnidentifiedAccessChecksum.generateFor(accountProfile.getUnidentifiedAccessKey()),
        accountProfile.isUnrestrictedUnidentifiedAccess(),
        UserCapabilities.createForAccount(accountProfile),
        username,
        accountProfile.getUuid(),
        profileBadgeConverter.convert(
            getAcceptableLanguagesForRequest(containerRequestContext),
            accountProfile.getBadges(),
            isSelf),
        null,
        null);
  }

  private ProfileKeyCredentialResponse getProfileCredential(final String encodedProfileCredentialRequest,
      final VersionedProfile profile,
      final UUID uuid) {
    try {
      final ProfileKeyCommitment commitment = new ProfileKeyCommitment(profile.getCommitment());
      final ProfileKeyCredentialRequest request = new ProfileKeyCredentialRequest(Hex.decodeHex(encodedProfileCredentialRequest));

      return zkProfileOperations.issueProfileKeyCredential(request, uuid, commitment);
    } catch (DecoderException | VerificationFailedException | InvalidInputException e) {
      throw new WebApplicationException(e, Response.status(Response.Status.BAD_REQUEST).build());
    }
  }

  private PniCredentialResponse getPniCredential(final String encodedCredentialRequest,
      final VersionedProfile profile,
      final UUID accountIdentifier,
      final UUID phoneNumberIdentifier) {

    try {
      final ProfileKeyCommitment commitment = new ProfileKeyCommitment(profile.getCommitment());
      final ProfileKeyCredentialRequest request = new ProfileKeyCredentialRequest(Hex.decodeHex(encodedCredentialRequest));

      return zkProfileOperations.issuePniCredential(request, accountIdentifier, phoneNumberIdentifier, commitment);
    } catch (DecoderException | VerificationFailedException | InvalidInputException e) {
      throw new WebApplicationException(e, Response.status(Response.Status.BAD_REQUEST).build());
    }
  }

  // Although clients should generally be using versioned profiles wherever possible, there are still a few lingering
  // use cases for getting profiles without a version (e.g. getting a contact's unidentified access key checksum).
  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{identifier}")
  public Profile getUnversionedProfile(
      @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @HeaderParam("User-Agent") String userAgent,
      @PathParam("identifier") UUID identifier,
      @QueryParam("ca") boolean useCaCertificate)
      throws RateLimitExceededException {

    if (auth.isEmpty() && accessKey.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    boolean isSelf = false;
    if (auth.isPresent()) {
      UUID authedUuid = auth.get().getAccount().getUuid();
      rateLimiters.getProfileLimiter().validate(authedUuid);
      isSelf = authedUuid.equals(identifier);
    }

    Optional<Account> accountProfile = accountsManager.getByAccountIdentifier(identifier);
    OptionalAccess.verify(auth.map(AuthenticatedAccount::getAccount), accessKey, accountProfile);

    Optional<String> username = accountProfile.flatMap(Account::getUsername);

    return new Profile(
        accountProfile.get().getProfileName(),
        null,
        null,
        accountProfile.get().getAvatar(),
        null,
        accountProfile.get().getIdentityKey(),
        UnidentifiedAccessChecksum.generateFor(accountProfile.get().getUnidentifiedAccessKey()),
        accountProfile.get().isUnrestrictedUnidentifiedAccess(),
        UserCapabilities.createForAccount(accountProfile.get()),
        username.orElse(null),
        null,
        profileBadgeConverter.convert(
            getAcceptableLanguagesForRequest(containerRequestContext),
            accountProfile.get().getBadges(),
            isSelf),
        null,
        null);
  }

  private ProfileAvatarUploadAttributes generateAvatarUploadForm(String objectName) {
    ZonedDateTime        now            = ZonedDateTime.now(ZoneOffset.UTC);
    Pair<String, String> policy         = policyGenerator.createFor(now, objectName, 10 * 1024 * 1024);
    String               signature      = policySigner.getSignature(now, policy.second());

    return new ProfileAvatarUploadAttributes(objectName, policy.first(), "private", "AWS4-HMAC-SHA256",
                                             now.format(PostPolicyGenerator.AWS_DATE_TIME), policy.second(), signature);

  }

  private String generateAvatarObjectName() {
    byte[] object = new byte[16];
    new SecureRandom().nextBytes(object);

    return "profiles/" + Base64.encodeBase64URLSafeString(object);
  }

  private List<Locale> getAcceptableLanguagesForRequest(ContainerRequestContext containerRequestContext) {
    try {
      return containerRequestContext.getAcceptableLanguages();
    } catch (final ProcessingException e) {
      logger.warn("Could not get acceptable languages", e);
      return List.of();
    }
  }

  private List<AccountBadge> mergeBadgeIdsWithExistingAccountBadges(
      final List<String> badgeIds,
      final List<AccountBadge> accountBadges) {
    LinkedHashMap<String, AccountBadge> existingBadges = new LinkedHashMap<>(accountBadges.size());
    for (final AccountBadge accountBadge : accountBadges) {
      existingBadges.putIfAbsent(accountBadge.getId(), accountBadge);
    }

    LinkedHashMap<String, AccountBadge> result = new LinkedHashMap<>(accountBadges.size());
    for (final String badgeId : badgeIds) {

      // duplicate in the list, ignore it
      if (result.containsKey(badgeId)) {
        continue;
      }

      // This is for testing badges and allows them to be added to an account at any time with an expiration of 1 day
      // in the future.
      BadgeConfiguration badgeConfiguration = badgeConfigurationMap.get(badgeId);
      if (badgeConfiguration != null && badgeConfiguration.isTestBadge()) {
        result.put(badgeId, new AccountBadge(badgeId, clock.instant().plus(Duration.ofDays(1)), true));
        continue;
      }

      // reordering or making visible existing badges
      if (existingBadges.containsKey(badgeId)) {
        AccountBadge accountBadge = existingBadges.get(badgeId).withVisibility(true);
        result.put(badgeId, accountBadge);
      }
    }

    // take any remaining account badges and make them invisible
    for (final Entry<String, AccountBadge> entry : existingBadges.entrySet()) {
      if (!result.containsKey(entry.getKey())) {
        AccountBadge accountBadge = entry.getValue().withVisibility(false);
        result.put(accountBadge.getId(), accountBadge);
      }
    }

    return new ArrayList<>(result.values());
  }
}
