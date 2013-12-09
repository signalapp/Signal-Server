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

import com.amazonaws.HttpMethod;
import com.yammer.dropwizard.auth.Auth;
import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptor;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.UrlSigner;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;


@Path("/v1/attachments")
public class AttachmentController {

  private final Logger logger = LoggerFactory.getLogger(AttachmentController.class);

  private final RateLimiters           rateLimiters;
  private final FederatedClientManager federatedClientManager;
  private final UrlSigner              urlSigner;

  public AttachmentController(RateLimiters rateLimiters,
                              FederatedClientManager federatedClientManager,
                              UrlSigner urlSigner)
  {
    this.rateLimiters           = rateLimiters;
    this.federatedClientManager = federatedClientManager;
    this.urlSigner              = urlSigner;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response allocateAttachment(@Auth Account account) throws RateLimitExceededException {
    rateLimiters.getAttachmentLimiter().validate(account.getNumber());

    long                 attachmentId = generateAttachmentId();
    URL                  url          = urlSigner.getPreSignedUrl(attachmentId, HttpMethod.PUT);
    AttachmentDescriptor descriptor   = new AttachmentDescriptor(attachmentId, url.toExternalForm());

    return Response.ok().entity(descriptor).build();
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{attachmentId}")
  public Response redirectToAttachment(@Auth                      Account account,
                                       @PathParam("attachmentId") long attachmentId,
                                       @QueryParam("relay")       String relay)
  {
    try {
      URL url;

      if (relay == null) url = urlSigner.getPreSignedUrl(attachmentId, HttpMethod.GET);
      else               url = federatedClientManager.getClient(relay).getSignedAttachmentUri(attachmentId);

      return Response.ok().entity(new AttachmentUri(url)).build();
    } catch (IOException e) {
      logger.warn("No conectivity", e);
      return Response.status(500).build();
    } catch (NoSuchPeerException e) {
      logger.info("No such peer: " + relay);
      return Response.status(404).build();
    }
  }

  private long generateAttachmentId() {
    try {
      byte[] attachmentBytes = new byte[8];
      SecureRandom.getInstance("SHA1PRNG").nextBytes(attachmentBytes);

      attachmentBytes[0] = (byte)(attachmentBytes[0] & 0x7F);
      return Conversions.byteArrayToLong(attachmentBytes);
    } catch (NoSuchAlgorithmException nsae) {
      throw new AssertionError(nsae);
    }
  }
}
