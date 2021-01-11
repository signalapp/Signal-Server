/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.ClientContactTokens;
import org.whispersystems.textsecuregcm.entities.ClientContacts;
import org.whispersystems.textsecuregcm.entities.DirectoryFeedbackRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Constants;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.auth.Auth;

@Path("/v1/directory")
public class DirectoryController {

  private static final String[] FEEDBACK_STATUSES = {
      "ok",
      "mismatch",
      "attestation-error",
      "unexpected-error",
  };

  private static final String[] FRONTED_REGIONS = {"+20", "+971", "+968", "+974"};

  private final Logger         logger            = LoggerFactory.getLogger(DirectoryController.class);
  private final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Histogram      contactsHistogram = metricRegistry.histogram(name(getClass(), "contacts"));
  private final Meter          contactsMeter     = metricRegistry.meter(name(getClass(), "contactRate"));

  private final Map<String, Meter> iosFeedbackMeters = new HashMap<String, Meter>() {{
    for (String status : FEEDBACK_STATUSES) {
      put(status, metricRegistry.meter(name(DirectoryController.class, "feedback-v2", "ios", status)));
    }
  }};
  private final Map<String, Meter> androidFeedbackMeters = new HashMap<String, Meter>() {{
    for (String status : FEEDBACK_STATUSES) {
      put(status, metricRegistry.meter(name(DirectoryController.class, "feedback-v2", "android", status)));
    }
  }};
  private final Map<String, Meter> unknownFeedbackMeters = new HashMap<String, Meter>() {{
    for (String status : FEEDBACK_STATUSES) {
      put(status, metricRegistry.meter(name(DirectoryController.class, "feedback-v2", "unknown", status)));
    }
  }};

  private final RateLimiters                       rateLimiters;
  private final DirectoryManager                   directory;
  private final ExternalServiceCredentialGenerator directoryServiceTokenGenerator;

  public DirectoryController(RateLimiters rateLimiters,
                             DirectoryManager directory,
                             ExternalServiceCredentialGenerator userTokenGenerator)
  {
    this.directory                      = directory;
    this.rateLimiters                   = rateLimiters;
    this.directoryServiceTokenGenerator = userTokenGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAuthToken(@Auth Account account) {
    return Response.ok().entity(directoryServiceTokenGenerator.generateFor(account.getNumber())).build();
  }

  @PUT
  @Path("/feedback-v3/{status}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setFeedback(@Auth                Account                  account,
                              @PathParam("status") String                   status,
                              @Valid               DirectoryFeedbackRequest request)
  {
    Map<String, Meter> platformFeedbackMeters = unknownFeedbackMeters;

    Optional<Device> masterDevice = account.getMasterDevice();
    if (masterDevice.isPresent()) {
      if (masterDevice.get().getApnId() != null) {
        platformFeedbackMeters = iosFeedbackMeters;
      } else if (masterDevice.get().getGcmId() != null) {
        platformFeedbackMeters = androidFeedbackMeters;
      }
    }

    Optional<Meter> meter = Optional.ofNullable(platformFeedbackMeters.get(status));
    if (meter.isPresent()) {
      meter.get().mark();

      return Response.ok().build();
    } else {
      return Response.status(Status.NOT_FOUND).build();
    }
  }


  @Timed
  @GET
  @Path("/{token}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTokenPresence(@Auth Account account, @PathParam("token") String token)
      throws RateLimitExceededException
  {
    return Response.status(429).build();
  }

  @Timed
  @PUT
  @Path("/tokens")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getContactIntersection(@Auth Account account,
                                               @HeaderParam("X-Forwarded-For") String forwardedFor,
                                               @Valid ClientContactTokens contacts)
      throws RateLimitExceededException
  {
    return Response.status(429).build();
  }

  private byte[] decodeToken(String encoded) throws IOException {
    return Base64.decodeWithoutPadding(encoded.replace('-', '+').replace('_', '/'));
  }
}
