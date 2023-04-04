package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.CallLinkConfiguration;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

@Path("/v1/call-link")
@io.swagger.v3.oas.annotations.tags.Tag(name = "CallLink")
public class CallLinkController {
  @VisibleForTesting
  public static final String ANONYMOUS_CREDENTIAL_PREFIX = "anon";

  private final ExternalServiceCredentialsGenerator callingFrontendServiceCredentialGenerator;

  public CallLinkController(
      ExternalServiceCredentialsGenerator callingFrontendServiceCredentialGenerator
  ) {
    this.callingFrontendServiceCredentialGenerator = callingFrontendServiceCredentialGenerator;
  }

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final CallLinkConfiguration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUsernameTimestampTruncatorAndPrefix(timestamp -> timestamp.truncatedTo(ChronoUnit.DAYS), ANONYMOUS_CREDENTIAL_PREFIX)
        .build();
  }
  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for calling frontend",
      description = """
          These credentials enable clients to prove to calling frontend that they were a Signal user within the last day.
          For client privacy, timestamps are truncated to 1 day granularity and the token does not include or derive from an ACI.
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public ExternalServiceCredentials getAuth(@Auth AuthenticatedAccount auth) {
    return callingFrontendServiceCredentialGenerator.generateWithTimestampAsUsername();
  }
}
