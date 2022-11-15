/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.vavr.Tuple;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.PniCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
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
import org.whispersystems.textsecuregcm.entities.BaseProfileResponse;
import org.whispersystems.textsecuregcm.entities.BatchIdentityCheckRequest;
import org.whispersystems.textsecuregcm.entities.BatchIdentityCheckResponse;
import org.whispersystems.textsecuregcm.entities.CreateProfileRequest;
import org.whispersystems.textsecuregcm.entities.CredentialProfileResponse;
import org.whispersystems.textsecuregcm.entities.ExpiringProfileKeyCredentialProfileResponse;
import org.whispersystems.textsecuregcm.entities.PniCredentialProfileResponse;
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.entities.ProfileKeyCredentialProfileResponse;
import org.whispersystems.textsecuregcm.entities.UserCapabilities;
import org.whispersystems.textsecuregcm.entities.VersionedProfileResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
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

  private final Executor batchIdentityCheckExecutor;

  @VisibleForTesting
  static final Duration EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION = Duration.ofDays(7);

  private static final String PROFILE_KEY_CREDENTIAL_TYPE = "profileKey";
  private static final String PNI_CREDENTIAL_TYPE = "pni";
  private static final String EXPIRING_PROFILE_KEY_CREDENTIAL_TYPE = "expiringProfileKey";

  private static final Counter VERSION_NOT_FOUND_COUNTER = Metrics.counter(name(ProfileController.class, "versionNotFound"));
  private static final String INVALID_ACCEPT_LANGUAGE_COUNTER_NAME = name(ProfileController.class, "invalidAcceptLanguage");

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
      ServerZkProfileOperations zkProfileOperations,
      Executor batchIdentityCheckExecutor) {
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
    this.batchIdentityCheckExecutor = Preconditions.checkNotNull(batchIdentityCheckExecutor);
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setProfile(@Auth AuthenticatedAccount auth, @NotNull @Valid CreateProfileRequest request) {
    if (StringUtils.isNotBlank(request.getPaymentAddress())) {
      final boolean hasDisallowedPrefix =
          dynamicConfigurationManager.getConfiguration().getPaymentsConfiguration().getDisallowedPrefixes().stream()
              .anyMatch(prefix -> auth.getAccount().getNumber().startsWith(prefix));

      if (hasDisallowedPrefix) {
        return Response.status(Status.FORBIDDEN).build();
      }
    }

    Optional<VersionedProfile> currentProfile = profilesManager.get(auth.getAccount().getUuid(), request.getVersion());

    Optional<String> currentAvatar = Optional.empty();
    if (currentProfile.isPresent() && currentProfile.get().getAvatar() != null && currentProfile.get().getAvatar().startsWith("profiles/")) {
      currentAvatar = Optional.of(currentProfile.get().getAvatar());
    }

    String avatar = switch (request.getAvatarChange()) {
      case UNCHANGED -> currentAvatar.orElse(null);
      case CLEAR -> null;
      case UPDATE -> generateAvatarObjectName();
    };

    profilesManager.set(auth.getAccount().getUuid(),
        new VersionedProfile(
            request.getVersion(),
            request.getName(),
            avatar,
            request.getAboutEmoji(),
            request.getAbout(),
            request.getPaymentAddress(),
            request.getCommitment().serialize()));

    if (request.getAvatarChange() != CreateProfileRequest.AvatarChange.UNCHANGED) {
      currentAvatar.ifPresent(s -> s3client.deleteObject(DeleteObjectRequest.builder()
          .bucket(bucket)
          .key(s)
          .build()));
    }

    List<AccountBadge> updatedBadges = request.getBadges()
        .map(badges -> mergeBadgeIdsWithExistingAccountBadges(badges, auth.getAccount().getBadges()))
        .orElseGet(() -> auth.getAccount().getBadges());

    accountsManager.update(auth.getAccount(), a -> {
      a.setBadges(clock, updatedBadges);
      a.setCurrentProfileVersion(request.getVersion());
    });

    if (request.getAvatarChange() == CreateProfileRequest.AvatarChange.UPDATE) {
      return Response.ok(generateAvatarUploadForm(avatar)).build();
    } else {
      return Response.ok().build();
    }
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{uuid}/{version}")
  public VersionedProfileResponse getProfile(
      @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("uuid") UUID uuid,
      @PathParam("version") String version)
      throws RateLimitExceededException {

    final Optional<Account> maybeRequester = auth.map(AuthenticatedAccount::getAccount);
    final Account targetAccount = verifyPermissionToReceiveAccountIdentityProfile(maybeRequester, accessKey, uuid);

    return buildVersionedProfileResponse(targetAccount,
        version,
        isSelfProfileRequest(maybeRequester, uuid),
        containerRequestContext);
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{uuid}/{version}/{credentialRequest}")
  public CredentialProfileResponse getProfile(
      @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("uuid") UUID uuid,
      @PathParam("version") String version,
      @PathParam("credentialRequest") String credentialRequest,
      @QueryParam("credentialType") @DefaultValue(PROFILE_KEY_CREDENTIAL_TYPE) String credentialType)
      throws RateLimitExceededException {

    final Optional<Account> maybeRequester = auth.map(AuthenticatedAccount::getAccount);
    final Account targetAccount = verifyPermissionToReceiveAccountIdentityProfile(maybeRequester, accessKey, uuid);
    final boolean isSelf = isSelfProfileRequest(maybeRequester, uuid);

    switch (credentialType) {
      case PROFILE_KEY_CREDENTIAL_TYPE -> {
        return buildProfileKeyCredentialProfileResponse(targetAccount,
            version,
            credentialRequest,
            isSelf,
            containerRequestContext);
      }

      case PNI_CREDENTIAL_TYPE -> {
        if (!isSelf) {
          throw new ForbiddenException();
        }

        return buildPniCredentialProfileResponse(targetAccount,
            version,
            credentialRequest,
            containerRequestContext);
      }

      case EXPIRING_PROFILE_KEY_CREDENTIAL_TYPE -> {
        return buildExpiringProfileKeyCredentialProfileResponse(targetAccount,
            version,
            credentialRequest,
            isSelf,
            Instant.now().plus(EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION).truncatedTo(ChronoUnit.DAYS),
            containerRequestContext);
      }

      default -> throw new BadRequestException();
    }
  }

  // Although clients should generally be using versioned profiles wherever possible, there are still a few lingering
  // use cases for getting profiles without a version (e.g. getting a contact's unidentified access key checksum).
  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{identifier}")
  public BaseProfileResponse getUnversionedProfile(
      @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,
      @PathParam("identifier") UUID identifier,
      @QueryParam("ca") boolean useCaCertificate)
      throws RateLimitExceededException {

    final Optional<Account> maybeAccountByPni = accountsManager.getByPhoneNumberIdentifier(identifier);
    final Optional<Account> maybeRequester = auth.map(AuthenticatedAccount::getAccount);

    final BaseProfileResponse profileResponse;

    if (maybeAccountByPni.isPresent()) {
      if (maybeRequester.isEmpty()) {
        throw new WebApplicationException(Response.Status.UNAUTHORIZED);
      } else {
        rateLimiters.getProfileLimiter().validate(maybeRequester.get().getUuid());
      }

      OptionalAccess.verify(maybeRequester, Optional.empty(), maybeAccountByPni);

      profileResponse = buildBaseProfileResponseForPhoneNumberIdentity(maybeAccountByPni.get());
    } else {
      final Account targetAccount = verifyPermissionToReceiveAccountIdentityProfile(maybeRequester, accessKey, identifier);

      profileResponse = buildBaseProfileResponseForAccountIdentity(targetAccount,
          isSelfProfileRequest(maybeRequester, identifier),
          containerRequestContext);
    }

    return profileResponse;
  }

  @Timed
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/identity_check/batch")
  public CompletableFuture<BatchIdentityCheckResponse> runBatchIdentityCheck(@NotNull @Valid BatchIdentityCheckRequest request) {
    return CompletableFuture.supplyAsync(() -> {
          List<BatchIdentityCheckResponse.Element> responseElements = Collections.synchronizedList(new ArrayList<>());

          final int targetBatchCount = 10;
          // clamp the amount per batch to be in the closed range [30, 100]
          final int batchSize = Math.min(Math.max(request.elements().size() / targetBatchCount, 30), 100);
          // add 1 extra batch if there is any remainder to consume the final non-full batch
          final int batchCount =
              request.elements().size() / batchSize + (request.elements().size() % batchSize != 0 ? 1 : 0);

          @SuppressWarnings("rawtypes") CompletableFuture[] futures = new CompletableFuture[batchCount];
          for (int i = 0; i < batchCount; ++i) {
            List<BatchIdentityCheckRequest.Element> batch = request.elements()
                .subList(i * batchSize, Math.min((i + 1) * batchSize, request.elements().size()));
            futures[i] = CompletableFuture.runAsync(() -> {
              MessageDigest sha256;
              try {
                sha256 = MessageDigest.getInstance("SHA-256");
              } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e);
              }
              for (final BatchIdentityCheckRequest.Element element : batch) {
                checkFingerprintAndAdd(element, responseElements, sha256);
              }
            }, batchIdentityCheckExecutor);
          }

          return Tuple.of(futures, responseElements);
        }).thenCompose(tuple2 -> CompletableFuture.allOf(tuple2._1).thenApply((ignored) -> new BatchIdentityCheckResponse(tuple2._2)));
  }

  private void checkFingerprintAndAdd(BatchIdentityCheckRequest.Element element,
      Collection<BatchIdentityCheckResponse.Element> responseElements, MessageDigest md) {

    final Optional<Account> maybeAccount;
    final boolean usePhoneNumberIdentity;
    if (element.aci() != null) {
      maybeAccount = accountsManager.getByAccountIdentifier(element.aci());
      usePhoneNumberIdentity = false;
    } else {
      final Optional<Account> maybeAciAccount = accountsManager.getByAccountIdentifier(element.uuid());

      if (maybeAciAccount.isEmpty()) {
        maybeAccount = accountsManager.getByPhoneNumberIdentifier(element.uuid());
        usePhoneNumberIdentity = true;
      } else {
        maybeAccount = maybeAciAccount;
        usePhoneNumberIdentity = false;
      }
    }

    maybeAccount.ifPresent(account -> {
      if (account.getIdentityKey() == null || account.getPhoneNumberIdentityKey() == null) {
        return;
      }
      byte[] identityKeyBytes;
      try {
        identityKeyBytes = Base64.getDecoder().decode(usePhoneNumberIdentity ? account.getPhoneNumberIdentityKey()
            : account.getIdentityKey());
      } catch (IllegalArgumentException ignored) {
        return;
      }
      md.reset();
      byte[] digest = md.digest(identityKeyBytes);
      byte[] fingerprint = Util.truncate(digest, 4);

      if (!Arrays.equals(fingerprint, element.fingerprint())) {
        responseElements.add(new BatchIdentityCheckResponse.Element(element.aci(), element.uuid(), identityKeyBytes));
      }
    });
  }

  private ProfileKeyCredentialProfileResponse buildProfileKeyCredentialProfileResponse(final Account account,
      final String version,
      final String encodedCredentialRequest,
      final boolean isSelf,
      final ContainerRequestContext containerRequestContext) {

    final ProfileKeyCredentialResponse profileKeyCredentialResponse = profilesManager.get(account.getUuid(), version)
        .map(profile -> getProfileCredential(encodedCredentialRequest, profile, account.getUuid()))
        .orElse(null);

    return new ProfileKeyCredentialProfileResponse(
        buildVersionedProfileResponse(account, version, isSelf, containerRequestContext),
        profileKeyCredentialResponse);
  }

  private PniCredentialProfileResponse buildPniCredentialProfileResponse(final Account account,
      final String version,
      final String encodedCredentialRequest,
      final ContainerRequestContext containerRequestContext) {

    final PniCredentialResponse pniCredentialResponse = profilesManager.get(account.getUuid(), version)
        .map(profile -> getPniCredential(encodedCredentialRequest, profile, account.getUuid(), account.getPhoneNumberIdentifier()))
        .orElse(null);

    return new PniCredentialProfileResponse(
        buildVersionedProfileResponse(account, version, true, containerRequestContext),
        pniCredentialResponse);
  }

  private ExpiringProfileKeyCredentialProfileResponse buildExpiringProfileKeyCredentialProfileResponse(
      final Account account,
      final String version,
      final String encodedCredentialRequest,
      final boolean isSelf,
      final Instant expiration,
      final ContainerRequestContext containerRequestContext) {

    final ExpiringProfileKeyCredentialResponse expiringProfileKeyCredentialResponse = profilesManager.get(account.getUuid(), version)
        .map(profile -> getExpiringProfileKeyCredentialResponse(encodedCredentialRequest, profile, account.getUuid(), expiration))
        .orElse(null);

    return new ExpiringProfileKeyCredentialProfileResponse(
        buildVersionedProfileResponse(account, version, isSelf, containerRequestContext),
        expiringProfileKeyCredentialResponse);
  }

  private VersionedProfileResponse buildVersionedProfileResponse(final Account account,
      final String version,
      final boolean isSelf,
      final ContainerRequestContext containerRequestContext) {

    final Optional<VersionedProfile> maybeProfile = profilesManager.get(account.getUuid(), version);

    if (maybeProfile.isEmpty()) {
      // Hypothesis: this should basically never happen since clients can't delete versions
      VERSION_NOT_FOUND_COUNTER.increment();
    }

    final String name = maybeProfile.map(VersionedProfile::getName).orElse(null);
    final String about = maybeProfile.map(VersionedProfile::getAbout).orElse(null);
    final String aboutEmoji = maybeProfile.map(VersionedProfile::getAboutEmoji).orElse(null);
    final String avatar = maybeProfile.map(VersionedProfile::getAvatar).orElse(null);

    // Allow requests where either the version matches the latest version on Account or the latest version on Account
    // is empty to read the payment address.
    final String paymentAddress = maybeProfile
        .filter(p -> account.getCurrentProfileVersion().map(v -> v.equals(version)).orElse(true))
        .map(VersionedProfile::getPaymentAddress)
        .orElse(null);

    return new VersionedProfileResponse(
        buildBaseProfileResponseForAccountIdentity(account, isSelf, containerRequestContext),
        name, about, aboutEmoji, avatar, paymentAddress);
  }

  private BaseProfileResponse buildBaseProfileResponseForAccountIdentity(final Account account,
      final boolean isSelf,
      final ContainerRequestContext containerRequestContext) {

    return new BaseProfileResponse(account.getIdentityKey(),
        UnidentifiedAccessChecksum.generateFor(account.getUnidentifiedAccessKey()),
        account.isUnrestrictedUnidentifiedAccess(),
        UserCapabilities.createForAccount(account),
        profileBadgeConverter.convert(
            getAcceptableLanguagesForRequest(containerRequestContext),
            account.getBadges(),
            isSelf),
        account.getUuid());
  }

  private BaseProfileResponse buildBaseProfileResponseForPhoneNumberIdentity(final Account account) {
    return new BaseProfileResponse(account.getPhoneNumberIdentityKey(),
        null,
        false,
        UserCapabilities.createForAccount(account),
        Collections.emptyList(),
        account.getPhoneNumberIdentifier());
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

  private ExpiringProfileKeyCredentialResponse getExpiringProfileKeyCredentialResponse(
      final String encodedCredentialRequest,
      final VersionedProfile profile,
      final UUID accountIdentifier,
      final Instant expiration) {

    try {
      final ProfileKeyCommitment commitment = new ProfileKeyCommitment(profile.getCommitment());
      final ProfileKeyCredentialRequest request = new ProfileKeyCredentialRequest(Hex.decodeHex(encodedCredentialRequest));

      return zkProfileOperations.issueExpiringProfileKeyCredential(request, accountIdentifier, commitment, expiration);
    } catch (DecoderException | VerificationFailedException | InvalidInputException e) {
      throw new WebApplicationException(e, Response.status(Response.Status.BAD_REQUEST).build());
    }
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

    return "profiles/" + Base64.getUrlEncoder().encodeToString(object);
  }

  private List<Locale> getAcceptableLanguagesForRequest(ContainerRequestContext containerRequestContext) {
    try {
      return containerRequestContext.getAcceptableLanguages();
    } catch (final ProcessingException e) {
      final String userAgent = containerRequestContext.getHeaderString(HttpHeaders.USER_AGENT);
      Metrics.counter(INVALID_ACCEPT_LANGUAGE_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).increment();
      logger.debug("Could not get acceptable languages; Accept-Language: {}; User-Agent: {}",
          containerRequestContext.getHeaderString(HttpHeaders.ACCEPT_LANGUAGE),
          userAgent,
          e);

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

  /**
   * Verifies that the requester has permission to view the profile of the account identified by the given ACI.
   *
   * @param maybeRequester the authenticated account requesting the profile, if any
   * @param maybeAccessKey an anonymous access key for the target account
   * @param targetUuid the ACI of the target account
   *
   * @return the target account
   *
   * @throws RateLimitExceededException if the requester must wait before requesting the target account's profile
   * @throws NotFoundException if no account was found for the target ACI
   * @throws NotAuthorizedException if the requester is not authorized to receive the target account's profile or if the
   * requester was not authenticated and did not present an anonymous access key
   */
  private Account verifyPermissionToReceiveAccountIdentityProfile(final Optional<Account> maybeRequester,
      final Optional<Anonymous> maybeAccessKey,
      final UUID targetUuid) throws RateLimitExceededException {

    if (maybeRequester.isEmpty() && maybeAccessKey.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (maybeRequester.isPresent()) {
      rateLimiters.getProfileLimiter().validate(maybeRequester.get().getUuid());
    }

    final Optional<Account> maybeTargetAccount = accountsManager.getByAccountIdentifier(targetUuid);

    OptionalAccess.verify(maybeRequester, maybeAccessKey, maybeTargetAccount);
    assert maybeTargetAccount.isPresent();

    return maybeTargetAccount.get();
  }

  private boolean isSelfProfileRequest(final Optional<Account> maybeRequester, final UUID targetUuid) {
    return maybeRequester.map(requester -> requester.getUuid().equals(targetUuid)).orElse(false);
  }
}
