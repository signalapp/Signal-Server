package org.whispersystems.textsecuregcm.controllers;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.codahale.metrics.annotation.Timed;
import org.apache.commons.codec.binary.Base64;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.configuration.CdnConfiguration;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.entities.UserCapabilities;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.util.Pair;

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

  private final RateLimiters     rateLimiters;
  private final AccountsManager  accountsManager;
  private final UsernamesManager usernamesManager;

  private final PolicySigner        policySigner;
  private final PostPolicyGenerator policyGenerator;

  private final AmazonS3            s3client;
  private final String              bucket;

  public ProfileController(RateLimiters rateLimiters,
                           AccountsManager accountsManager,
                           UsernamesManager usernamesManager,
                           CdnConfiguration profilesConfiguration)
  {
    AWSCredentials         credentials         = new BasicAWSCredentials(profilesConfiguration.getAccessKey(), profilesConfiguration.getAccessSecret());
    AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

    this.rateLimiters       = rateLimiters;
    this.accountsManager    = accountsManager;
    this.usernamesManager   = usernamesManager;
    this.bucket             = profilesConfiguration.getBucket();
    this.s3client           = AmazonS3Client.builder()
                                            .withCredentials(credentialsProvider)
                                            .withRegion(profilesConfiguration.getRegion())
                                            .build();

    this.policyGenerator  = new PostPolicyGenerator(profilesConfiguration.getRegion(),
                                                    profilesConfiguration.getBucket(),
                                                    profilesConfiguration.getAccessKey());

    this.policySigner     = new PolicySigner(profilesConfiguration.getAccessSecret(),
                                             profilesConfiguration.getRegion());
  }

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
                       new UserCapabilities(accountProfile.get().isUuidAddressingSupported()),
                       username.orElse(null),
                       null);
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
                       new UserCapabilities(accountProfile.get().isUuidAddressingSupported()),
                       username,
                       accountProfile.get().getUuid());
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/name/{name}")
  public void setProfile(@Auth Account account, @PathParam("name") @UnwrapValidatedValue(true) @Length(min = 72,max= 72) Optional<String> name) {
    account.setProfileName(name.orElse(null));
    accountsManager.update(account);
  }


  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/avatar")
  public ProfileAvatarUploadAttributes getAvatarUploadForm(@Auth Account account) {
    String               previousAvatar = account.getAvatar();
    ZonedDateTime        now            = ZonedDateTime.now(ZoneOffset.UTC);
    String               objectName     = generateAvatarObjectName();
    Pair<String, String> policy         = policyGenerator.createFor(now, objectName, 10 * 1024 * 1024);
    String               signature      = policySigner.getSignature(now, policy.second());

    if (previousAvatar != null && previousAvatar.startsWith("profiles/")) {
      s3client.deleteObject(bucket, previousAvatar);
    }

    account.setAvatar(objectName);
    accountsManager.update(account);

    return new ProfileAvatarUploadAttributes(objectName, policy.first(), "private", "AWS4-HMAC-SHA256",
                                             now.format(PostPolicyGenerator.AWS_DATE_TIME), policy.second(), signature);
  }

  private String generateAvatarObjectName() {
    byte[] object = new byte[16];
    new SecureRandom().nextBytes(object);

    return "profiles/" + Base64.encodeBase64URLSafeString(object);
  }
}
