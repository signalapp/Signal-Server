package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfig;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfigList;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.util.Conversions;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import io.dropwizard.auth.Auth;

@Path("/v1/config")
public class RemoteConfigController {

  private final RemoteConfigsManager remoteConfigsManager;
  private final List<String>         configAuthTokens;

  public RemoteConfigController(RemoteConfigsManager remoteConfigsManager, List<String> configAuthTokens) {
    this.remoteConfigsManager = remoteConfigsManager;
    this.configAuthTokens      = configAuthTokens;
  }

  @Timed
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public UserRemoteConfigList getAll(@Auth Account account) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA1");

      return new UserRemoteConfigList(remoteConfigsManager.getAll().stream().map(config -> new UserRemoteConfig(config.getName(),
                                                                                                                isInBucket(digest, account.getUuid(),
                                                                                                                           config.getName().getBytes(),
                                                                                                                           config.getPercentage(),
                                                                                                                           config.getUuids())))
                                                          .collect(Collectors.toList()));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void set(@HeaderParam("Config-Token") String configToken, @Valid RemoteConfig config) {
    if (!isAuthorized(configToken)) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    remoteConfigsManager.set(config);
  }

  @Timed
  @DELETE
  @Path("/{name}")
  public void delete(@HeaderParam("Config-Token") String configToken, @PathParam("name") String name) {
    if (!isAuthorized(configToken)) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    remoteConfigsManager.delete(name);
  }

  @VisibleForTesting
  public static boolean isInBucket(MessageDigest digest, UUID uid, byte[] configName, int configPercentage, Set<UUID> uuidsInBucket) {
    if (uuidsInBucket.contains(uid)) return true;

    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uid.getMostSignificantBits());
    bb.putLong(uid.getLeastSignificantBits());

    digest.update(bb.array());

    byte[] hash   = digest.digest(configName);
    int    bucket = (int)(Math.abs(Conversions.byteArrayToLong(hash)) % 100);

    return bucket < configPercentage;
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean isAuthorized(String configToken) {
    return configToken != null && configAuthTokens.stream().anyMatch(authorized -> MessageDigest.isEqual(authorized.getBytes(), configToken.getBytes()));
  }

}
