package org.whispersystems.textsecuregcm.controllers;

import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.annotation.Timed;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.VerificationFailedException;
import org.signal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.zkgroup.profiles.ProfileKeyCredentialResponse;
import org.signal.zkgroup.profiles.ServerZkProfileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.entities.CreateProfileRequest;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.entities.UserCapabilities;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.Pair;

import javax.validation.Valid;
import javax.validation.valueextraction.Unwrapping;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import io.dropwizard.auth.Auth;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/profile")
public class ProfileController {

  private final Logger logger = LoggerFactory.getLogger(ProfileController.class);

  private final RateLimiters     rateLimiters;
  private final ProfilesManager  profilesManager;
  private final AccountsManager  accountsManager;
  private final UsernamesManager usernamesManager;

  private final PolicySigner              policySigner;
  private final PostPolicyGenerator       policyGenerator;
  private final ServerZkProfileOperations zkProfileOperations;
  private final boolean                   isZkEnabled;

  private final AmazonS3            s3client;
  private final String              bucket;

  public ProfileController(RateLimiters rateLimiters,
                           AccountsManager accountsManager,
                           ProfilesManager profilesManager,
                           UsernamesManager usernamesManager,
                           AmazonS3 s3client,
                           PostPolicyGenerator policyGenerator,
                           PolicySigner policySigner,
                           String bucket,
                           ServerZkProfileOperations zkProfileOperations,
                           boolean isZkEnabled)
  {
    this.rateLimiters        = rateLimiters;
    this.accountsManager     = accountsManager;
    this.profilesManager     = profilesManager;
    this.usernamesManager    = usernamesManager;
    this.zkProfileOperations = zkProfileOperations;
    this.bucket              = bucket;
    this.s3client            = s3client;
    this.policyGenerator     = policyGenerator;
    this.policySigner        = policySigner;
    this.isZkEnabled         = isZkEnabled;
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setProfile(@Auth Account account, @Valid CreateProfileRequest request) {
    if (!isZkEnabled) throw new WebApplicationException(Response.Status.NOT_FOUND);

    Optional<VersionedProfile>              currentProfile = profilesManager.get(account.getUuid(), request.getVersion());
    String                                  avatar         = request.isAvatar() ? generateAvatarObjectName() : null;
    Optional<ProfileAvatarUploadAttributes> response       = Optional.empty();

    profilesManager.set(account.getUuid(), new VersionedProfile(request.getVersion(), request.getName(), avatar, request.getCommitment().serialize()));

    if (request.isAvatar()) {
      Optional<String> currentAvatar = Optional.empty();

      if (currentProfile.isPresent() && currentProfile.get().getAvatar() != null && currentProfile.get().getAvatar().startsWith("profiles/")) {
        currentAvatar = Optional.of(currentProfile.get().getAvatar());
      }

      if (currentAvatar.isEmpty() && account.getAvatar() != null && account.getAvatar().startsWith("profiles/")) {
        currentAvatar = Optional.of(account.getAvatar());
      }

      currentAvatar.ifPresent(s -> s3client.deleteObject(bucket, s));

      response = Optional.of(generateAvatarUploadForm(avatar));
    }

    account.setProfileName(request.getName());
    if (avatar != null) account.setAvatar(avatar);
    accountsManager.update(account);

    if (response.isPresent()) return Response.ok(response).build();
    else                      return Response.ok().build();
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{uuid}/{version}")
  public Optional<Profile> getProfile(@Auth                                     Optional<Account>   requestAccount,
                                      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
                                      @PathParam("uuid")                        UUID uuid,
                                      @PathParam("version")                     String version)
      throws RateLimitExceededException
  {
    if (!isZkEnabled) throw new WebApplicationException(Response.Status.NOT_FOUND);
    return getVersionedProfile(requestAccount, accessKey, uuid, version, Optional.empty());
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{uuid}/{version}/{credentialRequest}")
  public Optional<Profile> getProfile(@Auth                                     Optional<Account> requestAccount,
                                      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
                                      @PathParam("uuid")                        UUID uuid,
                                      @PathParam("version")                     String version,
                                      @PathParam("credentialRequest")           String credentialRequest)
      throws RateLimitExceededException
  {
    if (!isZkEnabled) throw new WebApplicationException(Response.Status.NOT_FOUND);
    return getVersionedProfile(requestAccount, accessKey, uuid, version, Optional.of(credentialRequest));
  }

  @SuppressWarnings("OptionalIsPresent")
  private Optional<Profile> getVersionedProfile(Optional<Account> requestAccount,
                                                Optional<Anonymous> accessKey,
                                                UUID uuid,
                                                String version,
                                                Optional<String> credentialRequest)
      throws RateLimitExceededException
  {
    if (!isZkEnabled) throw new WebApplicationException(Response.Status.NOT_FOUND);

    try {
      if (!requestAccount.isPresent() && !accessKey.isPresent()) {
        throw new WebApplicationException(Response.Status.UNAUTHORIZED);
      }

      if (requestAccount.isPresent()) {
        rateLimiters.getProfileLimiter().validate(requestAccount.get().getNumber());
      }

      Optional<Account> accountProfile = accountsManager.get(uuid);
      OptionalAccess.verify(requestAccount, accessKey, accountProfile);

      assert(accountProfile.isPresent());

      Optional<String>           username = usernamesManager.get(accountProfile.get().getUuid());
      Optional<VersionedProfile> profile  = profilesManager.get(uuid, version);

      String                     name     = profile.map(VersionedProfile::getName).orElse(accountProfile.get().getProfileName());
      String                     avatar   = profile.map(VersionedProfile::getAvatar).orElse(accountProfile.get().getAvatar());
      
      Optional<ProfileKeyCredentialResponse> credential = getProfileCredential(credentialRequest, profile, uuid);

      return Optional.of(new Profile(name,
                                     avatar,
                                     accountProfile.get().getIdentityKey(),
                                     UnidentifiedAccessChecksum.generateFor(accountProfile.get().getUnidentifiedAccessKey()),
                                     accountProfile.get().isUnrestrictedUnidentifiedAccess(),
                                     new UserCapabilities(accountProfile.get().isUuidAddressingSupported(), accountProfile.get().isGroupsV2Supported()),
                                     username.orElse(null),
                                     null, credential.orElse(null)));
    } catch (InvalidInputException e) {
      logger.info("Bad profile request", e);
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }
  }


  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/username/{username}")
  public Profile getProfileByUsername(@Auth Account account, @PathParam("username") String username) throws RateLimitExceededException {
    rateLimiters.getUsernameLookupLimiter().validate(account.getUuid().toString());

    username = username.toLowerCase();

    Optional<UUID> uuid = usernamesManager.get(username);

    if (!uuid.isPresent()) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
    }

    Optional<Account> accountProfile = accountsManager.get(uuid.get());

    if (!accountProfile.isPresent()) {
      throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
    }

    return new Profile(accountProfile.get().getProfileName(),
                       accountProfile.get().getAvatar(),
                       accountProfile.get().getIdentityKey(),
                       UnidentifiedAccessChecksum.generateFor(accountProfile.get().getUnidentifiedAccessKey()),
                       accountProfile.get().isUnrestrictedUnidentifiedAccess(),
                       new UserCapabilities(accountProfile.get().isUuidAddressingSupported(), accountProfile.get().isGroupsV2Supported()),
                       username,
                       accountProfile.get().getUuid(), null);
  }

  private Optional<ProfileKeyCredentialResponse> getProfileCredential(Optional<String>           encodedProfileCredentialRequest,
                                                                      Optional<VersionedProfile> profile,
                                                                      UUID                       uuid)
      throws InvalidInputException
  {
    if (!encodedProfileCredentialRequest.isPresent()) return Optional.empty();
    if (!profile.isPresent())                         return Optional.empty();

    try {
      ProfileKeyCommitment         commitment = new ProfileKeyCommitment(profile.get().getCommitment());
      ProfileKeyCredentialRequest  request    = new ProfileKeyCredentialRequest(Hex.decodeHex(encodedProfileCredentialRequest.get()));
      ProfileKeyCredentialResponse response   = zkProfileOperations.issueProfileKeyCredential(request, uuid, commitment);

      return Optional.of(response);
    } catch (DecoderException | VerificationFailedException e) {
      throw new WebApplicationException(e, Response.status(Response.Status.BAD_REQUEST).build());
    }
  }


  // Old profile endpoints. Replaced by versioned profile endpoints (above)

  @Deprecated
  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/name/{name}")
  public void setProfile(@Auth Account account, @PathParam("name") @ExactlySize(value = {72, 108}, payload = {Unwrapping.Unwrap.class}) Optional<String> name) {
    account.setProfileName(name.orElse(null));
    accountsManager.update(account);
  }

  @Deprecated
  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{identifier}")
  public Profile getProfile(@Auth                                     Optional<Account>   requestAccount,
                            @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
                            @PathParam("identifier")                  AmbiguousIdentifier identifier,
                            @QueryParam("ca")                         boolean useCaCertificate)
      throws RateLimitExceededException
  {
    if (!requestAccount.isPresent() && !accessKey.isPresent()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (requestAccount.isPresent()) {
      rateLimiters.getProfileLimiter().validate(requestAccount.get().getNumber());
    }

    Optional<Account> accountProfile = accountsManager.get(identifier);
    OptionalAccess.verify(requestAccount, accessKey, accountProfile);

    Optional<String> username = Optional.empty();

    if (!identifier.hasNumber()) {
      //noinspection OptionalGetWithoutIsPresent
      username = usernamesManager.get(accountProfile.get().getUuid());
    }

    return new Profile(accountProfile.get().getProfileName(),
                       accountProfile.get().getAvatar(),
                       accountProfile.get().getIdentityKey(),
                       UnidentifiedAccessChecksum.generateFor(accountProfile.get().getUnidentifiedAccessKey()),
                       accountProfile.get().isUnrestrictedUnidentifiedAccess(),
                       new UserCapabilities(accountProfile.get().isUuidAddressingSupported(), accountProfile.get().isGroupsV2Supported()),
                       username.orElse(null),
                       null, null);
  }


  @Deprecated
  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/avatar")
  public ProfileAvatarUploadAttributes getAvatarUploadForm(@Auth Account account) {
    String                        previousAvatar                = account.getAvatar();
    String                        objectName                    = generateAvatarObjectName();
    ProfileAvatarUploadAttributes profileAvatarUploadAttributes = generateAvatarUploadForm(objectName);

    if (previousAvatar != null && previousAvatar.startsWith("profiles/")) {
      s3client.deleteObject(bucket, previousAvatar);
    }

    account.setAvatar(objectName);
    accountsManager.update(account);

    return profileAvatarUploadAttributes;
  }

  ////

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
}
