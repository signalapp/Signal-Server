/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.apache.commons.codec.DecoderException;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthorizationTokenGenerator;
import org.whispersystems.textsecuregcm.configuration.ContactDiscoveryConfiguration;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.ClientContactTokens;
import org.whispersystems.textsecuregcm.entities.ClientContacts;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Constants;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.auth.Auth;

@Path("/v1/directory")
public class DirectoryController {

  private final Logger         logger            = LoggerFactory.getLogger(DirectoryController.class);
  private final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Histogram      contactsHistogram = metricRegistry.histogram(name(getClass(), "contacts"));

  private final RateLimiters     rateLimiters;
  private final DirectoryManager directory;

  private final Optional<AuthorizationTokenGenerator> userTokenGenerator;

  public DirectoryController(RateLimiters rateLimiters,
                             DirectoryManager directory,
                             Optional<ContactDiscoveryConfiguration> cdsConfig)
  {
    this.directory    = directory;
    this.rateLimiters = rateLimiters;

    if (cdsConfig.isPresent()) {
      try {
        this.userTokenGenerator = Optional.of(new AuthorizationTokenGenerator(
                cdsConfig.get().getUserAuthenticationTokenSharedSecret(),
                Optional.of(cdsConfig.get().getUserAuthenticationTokenUserIdSecret())
        ));
      } catch (DecoderException e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      this.userTokenGenerator = Optional.absent();
    }
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAuthToken(@Auth Account account) {
    if (userTokenGenerator.isPresent()) {
      return Response.ok().entity(userTokenGenerator.get().generateFor(account.getNumber())).build();
    } else {
      return Response.status(404).build();
    }
  }

  @Timed
  @PUT
  @Path("/feedback/ok")
  public Response putFeedbackOk(@Auth Account account) {
    return Response.ok().build();
  }

  @Timed
  @PUT
  @Path("/feedback/mismatch")
  public Response putFeedbackMismatch(@Auth Account account) {
    return Response.ok().build();
  }

  @Timed
  @PUT
  @Path("/feedback/server-error")
  public Response putFeedbackServerError(@Auth Account account) {
    return Response.ok().build();
  }

  @Timed
  @PUT
  @Path("/feedback/client-error")
  public Response putFeedbackClientError(@Auth Account account) {
    return Response.ok().build();
  }

  @Timed
  @PUT
  @Path("/feedback/attestation-error")
  public Response putFeedbackAttestationError(@Auth Account account) {
    return Response.ok().build();
  }

  @Timed
  @PUT
  @Path("/feedback/unexpected-error")
  public Response putFeedbackUnexpectedError(@Auth Account account) {
    return Response.ok().build();
  }


  @Timed
  @GET
  @Path("/{token}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTokenPresence(@Auth Account account, @PathParam("token") String token)
      throws RateLimitExceededException
  {
    rateLimiters.getContactsLimiter().validate(account.getNumber());

    try {
      Optional<ClientContact> contact = directory.get(decodeToken(token));

      if (contact.isPresent()) return Response.ok().entity(contact.get()).build();
      else                     return Response.status(404).build();

    } catch (IOException e) {
      logger.info("Bad token", e);
      return Response.status(404).build();
    }
  }

  @Timed
  @PUT
  @Path("/tokens")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public ClientContacts getContactIntersection(@Auth Account account, @Valid ClientContactTokens contacts)
      throws RateLimitExceededException
  {
    rateLimiters.getContactsLimiter().validate(account.getNumber(), contacts.getContacts().size());
    contactsHistogram.update(contacts.getContacts().size());

    try {
      List<byte[]> tokens = new LinkedList<>();

      for (String encodedContact : contacts.getContacts()) {
        tokens.add(decodeToken(encodedContact));
      }

      List<ClientContact> intersection = directory.get(tokens);
      return new ClientContacts(intersection);
    } catch (IOException e) {
      logger.info("Bad token", e);
      throw new WebApplicationException(Response.status(400).build());
    }
  }

  private byte[] decodeToken(String encoded) throws IOException {
    return Base64.decodeWithoutPadding(encoded.replace('-', '+').replace('_', '/'));
  }
}
