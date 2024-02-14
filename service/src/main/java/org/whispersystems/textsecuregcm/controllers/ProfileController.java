/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.base.Preconditions;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.vavr.Tuple;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
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
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
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
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.entities.UserCapabilities;
import org.whispersystems.textsecuregcm.entities.VersionedProfileResponse;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
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
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.ProfileHelper;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.websocket.auth.Mutable;
import org.whispersystems.websocket.auth.ReadOnly;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/profile")
@Tag(name = "Profile")
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

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setProfile(@Mutable @Auth AuthenticatedAccount auth, @NotNull @Valid CreateProfileRequest request) {

    final Optional<VersionedProfile> currentProfile = profilesManager.get(auth.getAccount().getUuid(),
        request.version());

    if (request.paymentAddress() != null && request.paymentAddress().length != 0) {
      final boolean hasDisallowedPrefix =
          dynamicConfigurationManager.getConfiguration().getPaymentsConfiguration().getDisallowedPrefixes().stream()
              .anyMatch(prefix -> auth.getAccount().getNumber().startsWith(prefix));

      if (hasDisallowedPrefix && currentProfile.map(VersionedProfile::paymentAddress).isEmpty()) {
        return Response.status(Response.Status.FORBIDDEN).build();
      }
    }

    Optional<String> currentAvatar = Optional.empty();
    if (currentProfile.isPresent() && currentProfile.get().avatar() != null && currentProfile.get().avatar()
        .startsWith("profiles/")) {
      currentAvatar = Optional.of(currentProfile.get().avatar());
    }

    final String avatar = switch (request.getAvatarChange()) {
      case UNCHANGED -> currentAvatar.orElse(null);
      case CLEAR -> null;
      case UPDATE -> ProfileHelper.generateAvatarObjectName();
    };

    profilesManager.set(auth.getAccount().getUuid(),
        new VersionedProfile(
            request.version(),
            request.name(),
            avatar,
            request.aboutEmoji(),
            request.about(),
            request.paymentAddress(),
            request.phoneNumberSharing(),
            request.commitment().serialize()));

    if (request.getAvatarChange() != CreateProfileRequest.AvatarChange.UNCHANGED) {
      currentAvatar.ifPresent(s -> s3client.deleteObject(DeleteObjectRequest.builder()
          .bucket(bucket)
          .key(s)
          .build()));
    }

    final List<AccountBadge> updatedBadges = request.badges()
        .map(badges -> ProfileHelper.mergeBadgeIdsWithExistingAccountBadges(clock, badgeConfigurationMap, badges, auth.getAccount().getBadges()))
        .orElseGet(() -> auth.getAccount().getBadges());

    accountsManager.update(auth.getAccount(), a -> {
      a.setBadges(clock, updatedBadges);
      a.setCurrentProfileVersion(request.version());
    });

    if (request.getAvatarChange() == CreateProfileRequest.AvatarChange.UPDATE) {
      return Response.ok(generateAvatarUploadForm(avatar)).build();
    } else {
      return Response.ok().build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{identifier}/{version}")
  @ManagedAsync
  public VersionedProfileResponse getProfile(
      @ReadOnly @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("identifier") AciServiceIdentifier accountIdentifier,
      @PathParam("version") String version)
      throws RateLimitExceededException {

    final Optional<Account> maybeRequester = auth.map(AuthenticatedAccount::getAccount);
    final Account targetAccount = verifyPermissionToReceiveAccountIdentityProfile(maybeRequester, accessKey, accountIdentifier);

    return buildVersionedProfileResponse(targetAccount,
        version,
        maybeRequester.map(requester -> ProfileHelper.isSelfProfileRequest(requester.getUuid(), accountIdentifier)).orElse(false),
        containerRequestContext);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{identifier}/{version}/{credentialRequest}")
  public CredentialProfileResponse getProfile(
      @ReadOnly @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @PathParam("identifier") AciServiceIdentifier accountIdentifier,
      @PathParam("version") String version,
      @PathParam("credentialRequest") String credentialRequest,
      @QueryParam("credentialType") String credentialType)
      throws RateLimitExceededException {

    if (!EXPIRING_PROFILE_KEY_CREDENTIAL_TYPE.equals(credentialType)) {
      throw new BadRequestException();
    }

    final Optional<Account> maybeRequester = auth.map(AuthenticatedAccount::getAccount);
    final Account targetAccount = verifyPermissionToReceiveAccountIdentityProfile(maybeRequester, accessKey, accountIdentifier);
    final boolean isSelf = maybeRequester.map(requester -> ProfileHelper.isSelfProfileRequest(requester.getUuid(), accountIdentifier)).orElse(false);

    return buildExpiringProfileKeyCredentialProfileResponse(targetAccount,
        version,
        credentialRequest,
        isSelf,
        containerRequestContext);
  }

  // Although clients should generally be using versioned profiles wherever possible, there are still a few lingering
  // use cases for getting profiles without a version (e.g. getting a contact's unidentified access key checksum).
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{identifier}")
  @ManagedAsync
  public BaseProfileResponse getUnversionedProfile(
      @ReadOnly @Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) Optional<Anonymous> accessKey,
      @Context ContainerRequestContext containerRequestContext,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,
      @PathParam("identifier") ServiceIdentifier identifier,
      @QueryParam("ca") boolean useCaCertificate)
      throws RateLimitExceededException {

    final Optional<Account> maybeRequester = auth.map(AuthenticatedAccount::getAccount);

    return switch (identifier.identityType()) {
      case ACI -> {
        final AciServiceIdentifier aciServiceIdentifier = (AciServiceIdentifier) identifier;

        final Account targetAccount =
            verifyPermissionToReceiveAccountIdentityProfile(maybeRequester, accessKey, aciServiceIdentifier);

        yield buildBaseProfileResponseForAccountIdentity(targetAccount,
            maybeRequester.map(requester -> ProfileHelper.isSelfProfileRequest(requester.getUuid(), aciServiceIdentifier)).orElse(false),
            containerRequestContext);
      }
      case PNI -> {
        final Optional<Account> maybeAccountByPni = accountsManager.getByPhoneNumberIdentifier(identifier.uuid());

        if (maybeRequester.isEmpty()) {
          throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        } else {
          rateLimiters.getProfileLimiter().validate(maybeRequester.get().getUuid());
        }

        OptionalAccess.verify(maybeRequester, Optional.empty(), maybeAccountByPni);

        assert maybeAccountByPni.isPresent();
        yield buildBaseProfileResponseForPhoneNumberIdentity(maybeAccountByPni.get());
      }
    };
  }

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

    final ServiceIdentifier identifier = Objects.requireNonNullElse(element.uuid(), element.aci());
    final Optional<Account> maybeAccount = accountsManager.getByServiceIdentifier(identifier);

    maybeAccount.ifPresent(account -> {
      final IdentityKey identityKey = account.getIdentityKey(identifier.identityType());
      if (identityKey == null) {
        return;
      }

      md.reset();
      byte[] digest = md.digest(identityKey.serialize());
      byte[] fingerprint = Util.truncate(digest, 4);

      if (!Arrays.equals(fingerprint, element.fingerprint())) {
        responseElements.add(new BatchIdentityCheckResponse.Element(element.uuid(), element.aci(), identityKey));
      }
    });
  }

  private ExpiringProfileKeyCredentialProfileResponse buildExpiringProfileKeyCredentialProfileResponse(
      final Account account,
      final String version,
      final String encodedCredentialRequest,
      final boolean isSelf,
      final ContainerRequestContext containerRequestContext) {

    final ExpiringProfileKeyCredentialResponse expiringProfileKeyCredentialResponse = profilesManager.get(account.getUuid(), version)
        .map(profile -> {
          final ExpiringProfileKeyCredentialResponse profileKeyCredentialResponse;
          try {
            profileKeyCredentialResponse = ProfileHelper.getExpiringProfileKeyCredential(HexFormat.of().parseHex(encodedCredentialRequest),
                profile, new ServiceId.Aci(account.getUuid()), zkProfileOperations);
          } catch (VerificationFailedException | InvalidInputException e) {
            throw new BadRequestException(Response.status(Response.Status.BAD_REQUEST).build(), e);
          }
          return profileKeyCredentialResponse;
        })
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

    final byte[] name = maybeProfile.map(VersionedProfile::name).orElse(null);
    final byte[] about = maybeProfile.map(VersionedProfile::about).orElse(null);
    final byte[] aboutEmoji = maybeProfile.map(VersionedProfile::aboutEmoji).orElse(null);
    final String avatar = maybeProfile.map(VersionedProfile::avatar).orElse(null);
    final byte[] phoneNumberSharing = maybeProfile.map(VersionedProfile::phoneNumberSharing).orElse(null);

    // Allow requests where either the version matches the latest version on Account or the latest version on Account
    // is empty to read the payment address.
    final byte[] paymentAddress = maybeProfile
        .filter(p -> account.getCurrentProfileVersion().map(v -> v.equals(version)).orElse(true))
        .map(VersionedProfile::paymentAddress)
        .orElse(null);

    return new VersionedProfileResponse(
        buildBaseProfileResponseForAccountIdentity(account, isSelf, containerRequestContext),
        name, about, aboutEmoji, avatar, paymentAddress, phoneNumberSharing);
  }

  private BaseProfileResponse buildBaseProfileResponseForAccountIdentity(final Account account,
      final boolean isSelf,
      final ContainerRequestContext containerRequestContext) {

    return new BaseProfileResponse(account.getIdentityKey(IdentityType.ACI),
        account.getUnidentifiedAccessKey().map(UnidentifiedAccessChecksum::generateFor).orElse(null),
        account.isUnrestrictedUnidentifiedAccess(),
        UserCapabilities.createForAccount(account),
        profileBadgeConverter.convert(
            getAcceptableLanguagesForRequest(containerRequestContext),
            account.getBadges(),
            isSelf),
        new AciServiceIdentifier(account.getUuid()));
  }

  private BaseProfileResponse buildBaseProfileResponseForPhoneNumberIdentity(final Account account) {
    return new BaseProfileResponse(account.getIdentityKey(IdentityType.PNI),
        null,
        false,
        UserCapabilities.createForAccount(account),
        Collections.emptyList(),
        new PniServiceIdentifier(account.getPhoneNumberIdentifier()));
  }

  private List<Locale> getAcceptableLanguagesForRequest(final ContainerRequestContext containerRequestContext) {
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

  /**
   * Verifies that the requester has permission to view the profile of the account identified by the given ACI.
   *
   * @param maybeRequester the authenticated account requesting the profile, if any
   * @param maybeAccessKey an anonymous access key for the target account
   * @param accountIdentifier the ACI of the target account
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
      final AciServiceIdentifier accountIdentifier) throws RateLimitExceededException {

    if (maybeRequester.isEmpty() && maybeAccessKey.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (maybeRequester.isPresent()) {
      rateLimiters.getProfileLimiter().validate(maybeRequester.get().getUuid());
    }

    final Optional<Account> maybeTargetAccount = accountsManager.getByAccountIdentifier(accountIdentifier.uuid());

    OptionalAccess.verify(maybeRequester, maybeAccessKey, maybeTargetAccount);
    assert maybeTargetAccount.isPresent();

    return maybeTargetAccount.get();
  }

  private ProfileAvatarUploadAttributes generateAvatarUploadForm(
      final String objectName) {
    ZonedDateTime now = ZonedDateTime.now(clock);
    Pair<String, String> policy = policyGenerator.createFor(now, objectName, ProfileHelper.MAX_PROFILE_AVATAR_SIZE_BYTES);
    String signature = policySigner.getSignature(now, policy.second());

    return new ProfileAvatarUploadAttributes(objectName, policy.first(),
        "private", "AWS4-HMAC-SHA256",
        now.format(PostPolicyGenerator.AWS_DATE_TIME), policy.second(), signature);
  }

  @Nullable
  private static byte[] decodeFromBase64(@Nullable final String input) {
    if (input == null) {
      return null;
    }
    return Base64.getDecoder().decode(input);
  }

  @Nullable
  private static String encodeToBase64(@Nullable final byte[] input) {
    if (input == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(input);
  }
}
