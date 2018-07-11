package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import io.dropwizard.auth.Auth;
import org.apache.commons.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthorizationTokenGenerator;
import org.whispersystems.textsecuregcm.configuration.ContactDiscoveryConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Constants;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/contact-discovery")
public class ContactDiscoveryController {
  private final Logger         logger            = LoggerFactory.getLogger(DirectoryController.class);
  private final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private final AuthorizationTokenGenerator userTokenGenerator;

  public ContactDiscoveryController(ContactDiscoveryConfiguration cdsConfig) throws DecoderException {
    this.userTokenGenerator = new AuthorizationTokenGenerator(
            cdsConfig.getUserAuthenticationTokenSharedSecret(),
            Optional.of(cdsConfig.getUserAuthenticationTokenUserIdSecret())
    );
  }

  @Timed
  @GET
  @Path("/auth-token")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAuthToken(@Auth Account account) {
    return Response.ok().entity(userTokenGenerator.generateFor(account.getNumber())).build();
  }
}
