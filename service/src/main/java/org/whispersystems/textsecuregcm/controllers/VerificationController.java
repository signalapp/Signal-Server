/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.CreateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.entities.SubmitVerificationCodeRequest;
import org.whispersystems.textsecuregcm.entities.UpdateVerificationSessionRequest;
import org.whispersystems.textsecuregcm.entities.VerificationCodeRequest;
import org.whispersystems.textsecuregcm.entities.VerificationSessionResponse;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RegistrationServiceSenderExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.PushNotification;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.registration.ClientType;
import org.whispersystems.textsecuregcm.registration.MessageTransport;
import org.whispersystems.textsecuregcm.registration.RegistrationFraudException;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceException;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceSenderException;
import org.whispersystems.textsecuregcm.registration.TransportNotAllowedException;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.spam.RegistrationFraudChecker;
import org.whispersystems.textsecuregcm.spam.RegistrationFraudChecker.VerificationCheck;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.ObsoletePhoneNumberFormatException;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

@Path("/v1/verification")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Verification")
public class VerificationController {

  private static final Logger logger = LoggerFactory.getLogger(VerificationController.class);
  private static final Duration REGISTRATION_RPC_TIMEOUT = Duration.ofSeconds(15);
  private static final Duration DYNAMODB_TIMEOUT = Duration.ofSeconds(5);

  private static final SecureRandom RANDOM = new SecureRandom();

  private static final String PUSH_CHALLENGE_COUNTER_NAME = name(VerificationController.class, "pushChallenge");
  private static final String CHALLENGE_PRESENT_TAG_NAME = "present";
  private static final String CHALLENGE_MATCH_TAG_NAME = "matches";
  private static final String CAPTCHA_ATTEMPT_COUNTER_NAME = name(VerificationController.class, "captcha");
  private static final String COUNTRY_CODE_TAG_NAME = "countryCode";
  private static final String REGION_CODE_TAG_NAME = "regionCode";
  private static final String SCORE_TAG_NAME = "score";
  private static final String CODE_REQUESTED_COUNTER_NAME = name(VerificationController.class, "codeRequested");
  private static final String VERIFICATION_TRANSPORT_TAG_NAME = "transport";
  private static final String VERIFIED_COUNTER_NAME = name(VerificationController.class, "verified");
  private static final String SUCCESS_TAG_NAME = "success";

  private final RegistrationServiceClient registrationServiceClient;
  private final VerificationSessionManager verificationSessionManager;
  private final PushNotificationManager pushNotificationManager;
  private final RegistrationCaptchaManager registrationCaptchaManager;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final PhoneNumberIdentifiers phoneNumberIdentifiers;
  private final RateLimiters rateLimiters;
  private final AccountsManager accountsManager;
  private final RegistrationFraudChecker registrationFraudChecker;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final Clock clock;

  public VerificationController(final RegistrationServiceClient registrationServiceClient,
      final VerificationSessionManager verificationSessionManager,
      final PushNotificationManager pushNotificationManager,
      final RegistrationCaptchaManager registrationCaptchaManager,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final RateLimiters rateLimiters,
      final AccountsManager accountsManager,
      final RegistrationFraudChecker registrationFraudChecker,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final Clock clock) {
    this.registrationServiceClient = registrationServiceClient;
    this.verificationSessionManager = verificationSessionManager;
    this.pushNotificationManager = pushNotificationManager;
    this.registrationCaptchaManager = registrationCaptchaManager;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.rateLimiters = rateLimiters;
    this.accountsManager = accountsManager;
    this.registrationFraudChecker = registrationFraudChecker;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.clock = clock;
  }

  @POST
  @Path("/session")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Creates a new verification session for a specific phone number",
      description = """
          Initiates a session to be able to verify the phone number for account registration. Check the response and 
          submit requested information at PATCH /session/{sessionId}
          """)
  @ApiResponse(responseCode = "200", description = "The verification session was created successfully", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "422", description = "The request did not pass validation")
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed",
      schema = @Schema(implementation = Integer.class)))
  public VerificationSessionResponse createSession(@NotNull @Valid final CreateVerificationSessionRequest request,
      @Context final ContainerRequestContext requestContext)
      throws RateLimitExceededException, ObsoletePhoneNumberFormatException {

    final Pair<String, PushNotification.TokenType> pushTokenAndType = validateAndExtractPushToken(
        request.updateVerificationSessionRequest());

    final Phonenumber.PhoneNumber phoneNumber;
    try {
      phoneNumber = Util.canonicalizePhoneNumber(PhoneNumberUtil.getInstance().parse(request.number(), null));
    } catch (final NumberParseException e) {
      throw new ServerErrorException("could not parse already validated number", Response.Status.INTERNAL_SERVER_ERROR);
    }

    final RegistrationServiceSession registrationServiceSession;
    try {
      final String sourceHost = (String) requestContext.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

      registrationServiceSession = registrationServiceClient.createRegistrationSession(phoneNumber, sourceHost,
          accountsManager.getByE164(request.number()).isPresent(),
          REGISTRATION_RPC_TIMEOUT).join();
    } catch (final CancellationException e) {

      throw new ServerErrorException("registration service unavailable", Response.Status.SERVICE_UNAVAILABLE);
    } catch (final CompletionException e) {

      if (ExceptionUtils.unwrap(e) instanceof RateLimitExceededException re) {
        throw re;
      }

      throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    VerificationSession verificationSession = new VerificationSession(null, new ArrayList<>(),
        Collections.emptyList(), null, null, false,
        clock.millis(), clock.millis(), registrationServiceSession.expiration());

    verificationSession = handlePushToken(pushTokenAndType, verificationSession);
    // unconditionally request a captcha -- it will either be the only requested information, or a fallback
    // if a push challenge sent in `handlePushToken` doesn't arrive in time
    verificationSession.requestedInformation().add(VerificationSession.Information.CAPTCHA);

    storeVerificationSession(registrationServiceSession, verificationSession);

    return buildResponse(registrationServiceSession, verificationSession);
  }

  @PATCH
  @Path("/session/{sessionId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Update a registration verification session",
      description = """
          Updates the session with requested information like an answer to a push challenge or captcha. 
          If `requestedInformation` in the response is empty, and `allowedToRequestCode` is `true`, proceed to call 
          `POST /session/{sessionId}/code`. If `requestedInformation` is empty and `allowedToRequestCode` is `false`, 
          then the caller must create a new verification session.
          """)
  @ApiResponse(responseCode = "200", description = "Session was updated successfully with the information provided", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "403", description = "The information provided was not accepted (e.g push challenge or captcha verification failed)")
  @ApiResponse(responseCode = "422", description = "The request did not pass validation")
  @ApiResponse(responseCode = "429", description = "Too many attempts",
      content = @Content(schema = @Schema(implementation = VerificationSessionResponse.class)),
      headers = @Header(
          name = "Retry-After",
          description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed",
          schema = @Schema(implementation = Integer.class)))
  public VerificationSessionResponse updateSession(
      @PathParam("sessionId") final String encodedSessionId,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @Context final ContainerRequestContext requestContext,
      @NotNull @Valid final UpdateVerificationSessionRequest updateVerificationSessionRequest) {

    final String sourceHost = (String) requestContext.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

    final Pair<String, PushNotification.TokenType> pushTokenAndType = validateAndExtractPushToken(
        updateVerificationSessionRequest);

    final RegistrationServiceSession registrationServiceSession = retrieveRegistrationServiceSession(encodedSessionId);
    VerificationSession verificationSession = retrieveVerificationSession(registrationServiceSession);

    final VerificationCheck verificationCheck = registrationFraudChecker.checkVerificationAttempt(
        requestContext,
        verificationSession,
        registrationServiceSession.number(),
        updateVerificationSessionRequest);

    try {
      // these handle* methods ordered from least likely to fail to most, so take care when considering a change

      verificationSession = verificationCheck.updatedSession().orElse(verificationSession);

      verificationSession = handlePushToken(pushTokenAndType, verificationSession);

      verificationSession = handlePushChallenge(updateVerificationSessionRequest, registrationServiceSession,
          verificationSession);

      verificationSession = handleCaptcha(sourceHost, updateVerificationSessionRequest, registrationServiceSession,
          verificationSession, userAgent, verificationCheck.scoreThreshold());
    } catch (final RateLimitExceededException e) {

      final Response response = buildResponseForRateLimitExceeded(verificationSession, registrationServiceSession,
          e.getRetryDuration());
      throw new ClientErrorException(response);

    } catch (final ForbiddenException e) {

      throw new ClientErrorException(Response.status(Response.Status.FORBIDDEN)
          .entity(buildResponse(registrationServiceSession, verificationSession))
          .build());

    } finally {
      // Each of the handle* methods may update requestedInformation, submittedInformation, and allowedToRequestCode,
      // and we want to be sure to store a changes, even if a later method throws
      updateStoredVerificationSession(registrationServiceSession, verificationSession);
    }

    return buildResponse(registrationServiceSession, verificationSession);
  }

  private void storeVerificationSession(final RegistrationServiceSession registrationServiceSession,
      final VerificationSession verificationSession) {
    verificationSessionManager.insert(registrationServiceSession.encodedSessionId(), verificationSession)
        .orTimeout(DYNAMODB_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
        .join();
  }

  private void updateStoredVerificationSession(final RegistrationServiceSession registrationServiceSession,
      final VerificationSession verificationSession) {
    verificationSessionManager.update(registrationServiceSession.encodedSessionId(), verificationSession)
        .orTimeout(DYNAMODB_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
        .join();
  }

  /**
   * If {@code pushTokenAndType} values are not {@code null}, sends a push challenge. If there is no existing push
   * challenge in the session, one will be created, set on the returned session record, and
   * {@link VerificationSession#requestedInformation()} will be updated.
   */
  private VerificationSession handlePushToken(
      final Pair<String, PushNotification.TokenType> pushTokenAndType, VerificationSession verificationSession) {

    if (pushTokenAndType.first() != null) {

      if (verificationSession.pushChallenge() == null) {

        final List<VerificationSession.Information> requestedInformation = new ArrayList<>();
        requestedInformation.add(VerificationSession.Information.PUSH_CHALLENGE);
        requestedInformation.addAll(verificationSession.requestedInformation());

        verificationSession = new VerificationSession(generatePushChallenge(), requestedInformation,
            verificationSession.submittedInformation(), verificationSession.smsSenderOverride(),
            verificationSession.voiceSenderOverride(), verificationSession.allowedToRequestCode(),
            verificationSession.createdTimestamp(), clock.millis(), verificationSession.remoteExpirationSeconds()
        );
      }

      pushNotificationManager.sendRegistrationChallengeNotification(pushTokenAndType.first(), pushTokenAndType.second(),
          verificationSession.pushChallenge());
    }

    return verificationSession;
  }

  /**
   * If a push challenge value is present, compares against the stored value. If they match, then
   * {@link VerificationSession.Information#PUSH_CHALLENGE} is removed from requested information, added to submitted
   * information, and {@link VerificationSession#allowedToRequestCode()} is re-evaluated.
   *
   * @throws ForbiddenException         if values to not match.
   * @throws RateLimitExceededException if too many push challenges have been submitted
   */
  private VerificationSession handlePushChallenge(
      final UpdateVerificationSessionRequest updateVerificationSessionRequest,
      final RegistrationServiceSession registrationServiceSession,
      VerificationSession verificationSession) throws RateLimitExceededException {

    if (verificationSession.submittedInformation()
        .contains(VerificationSession.Information.PUSH_CHALLENGE)) {
      // skip if a challenge has already been submitted
      return verificationSession;
    }

    final boolean pushChallengePresent = updateVerificationSessionRequest.pushChallenge() != null;
    if (pushChallengePresent) {
      rateLimiters.getVerificationPushChallengeLimiter()
          .validate(registrationServiceSession.encodedSessionId());
    }

    final boolean pushChallengeMatches;
    if (pushChallengePresent && verificationSession.pushChallenge() != null) {
      pushChallengeMatches = MessageDigest.isEqual(
          updateVerificationSessionRequest.pushChallenge().getBytes(StandardCharsets.UTF_8),
          verificationSession.pushChallenge().getBytes(StandardCharsets.UTF_8));
    } else {
      pushChallengeMatches = false;
    }

    Metrics.counter(PUSH_CHALLENGE_COUNTER_NAME,
            COUNTRY_CODE_TAG_NAME, Util.getCountryCode(registrationServiceSession.number()),
            REGION_CODE_TAG_NAME, Util.getRegion(registrationServiceSession.number()),
            CHALLENGE_PRESENT_TAG_NAME, Boolean.toString(pushChallengePresent),
            CHALLENGE_MATCH_TAG_NAME, Boolean.toString(pushChallengeMatches))
        .increment();

    if (pushChallengeMatches) {
      final List<VerificationSession.Information> submittedInformation = new ArrayList<>(
          verificationSession.submittedInformation());
      submittedInformation.add(VerificationSession.Information.PUSH_CHALLENGE);

      final List<VerificationSession.Information> requestedInformation = new ArrayList<>(
          verificationSession.requestedInformation());
      // a push challenge satisfies a requested captcha
      requestedInformation.remove(VerificationSession.Information.CAPTCHA);
      final boolean allowedToRequestCode = (verificationSession.allowedToRequestCode()
          || requestedInformation.remove(VerificationSession.Information.PUSH_CHALLENGE))
          && requestedInformation.isEmpty();

      verificationSession = new VerificationSession(verificationSession.pushChallenge(), requestedInformation,
          submittedInformation, verificationSession.smsSenderOverride(), verificationSession.voiceSenderOverride(),
          allowedToRequestCode, verificationSession.createdTimestamp(), clock.millis(),
          verificationSession.remoteExpirationSeconds());

    } else if (pushChallengePresent) {
      throw new ForbiddenException();
    }
    return verificationSession;
  }

  /**
   * If a captcha value is present, it is assessed. If it is valid, then {@link VerificationSession.Information#CAPTCHA}
   * is removed from requested information, added to submitted information, and
   * {@link VerificationSession#allowedToRequestCode()} is re-evaluated.
   *
   * @throws ForbiddenException         if assessment is not valid.
   * @throws RateLimitExceededException if too many captchas have been submitted
   */
  private VerificationSession handleCaptcha(
      final String sourceHost,
      final UpdateVerificationSessionRequest updateVerificationSessionRequest,
      final RegistrationServiceSession registrationServiceSession,
      VerificationSession verificationSession,
      final String userAgent,
      final Optional<Float> captchaScoreThreshold) throws RateLimitExceededException {

    if (updateVerificationSessionRequest.captcha() == null) {
      return verificationSession;
    }

    rateLimiters.getVerificationCaptchaLimiter().validate(registrationServiceSession.encodedSessionId());

    final AssessmentResult assessmentResult;
    try {

      assessmentResult = registrationCaptchaManager.assessCaptcha(
              Optional.empty(),
              Optional.of(updateVerificationSessionRequest.captcha()), sourceHost, userAgent)
          .orElseThrow(() -> new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR));

      Metrics.counter(CAPTCHA_ATTEMPT_COUNTER_NAME, Tags.of(
              Tag.of(SUCCESS_TAG_NAME, String.valueOf(assessmentResult.isValid(captchaScoreThreshold))),
              UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(registrationServiceSession.number())),
              Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(registrationServiceSession.number())),
              Tag.of(SCORE_TAG_NAME, assessmentResult.getScoreString())))
          .increment();

    } catch (final IOException e) {
      logger.error("error assessing captcha", e);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE, e);
    }

    if (assessmentResult.isValid(captchaScoreThreshold)) {
      final List<VerificationSession.Information> submittedInformation = new ArrayList<>(
          verificationSession.submittedInformation());
      submittedInformation.add(VerificationSession.Information.CAPTCHA);

      final List<VerificationSession.Information> requestedInformation = new ArrayList<>(
          verificationSession.requestedInformation());
      // a captcha satisfies a push challenge, in case of push deliverability issues
      requestedInformation.remove(VerificationSession.Information.PUSH_CHALLENGE);
      final boolean allowedToRequestCode = (verificationSession.allowedToRequestCode()
          || requestedInformation.remove(VerificationSession.Information.CAPTCHA))
          && requestedInformation.isEmpty();

      verificationSession = new VerificationSession(verificationSession.pushChallenge(), requestedInformation,
          submittedInformation, verificationSession.smsSenderOverride(), verificationSession.voiceSenderOverride(),
          allowedToRequestCode, verificationSession.createdTimestamp(), clock.millis(),
          verificationSession.remoteExpirationSeconds());
    } else {
      throw new ForbiddenException();
    }

    return verificationSession;
  }

  @GET
  @Path("/session/{sessionId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get a registration verification session",
      description = """
          Retrieve metadata of the registration verification session with the specified ID
          """)
  @ApiResponse(responseCode = "200", description = "Session was retrieved successfully", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid session ID")
  @ApiResponse(responseCode = "404", description = "Session with the specified ID could not be found")
  @ApiResponse(responseCode = "422", description = "Malformed session ID encoding")
  public VerificationSessionResponse getSession(@PathParam("sessionId") final String encodedSessionId) {

    final RegistrationServiceSession registrationServiceSession = retrieveRegistrationServiceSession(encodedSessionId);
    final VerificationSession verificationSession = retrieveVerificationSession(registrationServiceSession);

    return buildResponse(registrationServiceSession, verificationSession);
  }

  @POST
  @Path("/session/{sessionId}/code")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Request a verification code",
      description = """
          Sends a verification code to the phone number associated with the specified session via SMS or phone call.
          This endpoint can only be called when the session metadata includes "allowedToRequestCode = true"
          """)
  @ApiResponse(responseCode = "200", description = "Verification code was successfully sent", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid session ID")
  @ApiResponse(responseCode = "404", description = "Session with the specified ID could not be found")
  @ApiResponse(responseCode = "409", description = "The session is already verified or not in a state to request a code because requested information hasn't been provided yet",
      content = @Content(schema = @Schema(implementation = VerificationSessionResponse.class)))
  @ApiResponse(responseCode = "418", description = "The request to send a verification code with the given transport could not be fulfilled, but may succeed with a different transport",
      content = @Content(schema = @Schema(implementation = VerificationSessionResponse.class)))
  @ApiResponse(responseCode = "422", description = "Request did not pass validation")
  @ApiResponse(responseCode = "429", description = """
      Too may attempts; the caller is not permitted to send a verification code via the requested channel at this time 
      and may need to wait before trying again; if the session metadata does not specify a time at which the caller may 
      try again, then the caller has exhausted their permitted attempts and must either try a different transport or 
      create a new verification session.
      """,
      content = @Content(schema = @Schema(implementation = VerificationSessionResponse.class)),
      headers = @Header(
          name = "Retry-After",
          description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed",
          schema = @Schema(implementation = Integer.class)
      ))
  @ApiResponse(responseCode = "440", description = """
      The attempt to send a verification code failed because an external service (e.g. the SMS provider) refused to 
      deliver the code. This may be a temporary or permanent failure, as indicated in the response body. If temporary, 
      clients may try again after a reasonable delay. If permanent, clients should not retry the request and should 
      communicate the permanent failure to the end user. Permanent failures may result in the server disallowing all 
      future attempts to request or submit verification codes (since those attempts would be all but guaranteed to fail).
      """,
      content = @Content(schema = @Schema(implementation = RegistrationServiceSenderExceptionMapper.SendVerificationCodeFailureResponse.class)))
  public VerificationSessionResponse requestVerificationCode(@PathParam("sessionId") final String encodedSessionId,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @Parameter(in = ParameterIn.HEADER, description = "Ordered list of languages in which the client prefers to receive SMS or voice verification messages") @HeaderParam(HttpHeaders.ACCEPT_LANGUAGE)
      final Optional<String> acceptLanguage,
      @NotNull @Valid final VerificationCodeRequest verificationCodeRequest) throws Throwable {

    final RegistrationServiceSession registrationServiceSession = retrieveRegistrationServiceSession(encodedSessionId);
    final VerificationSession verificationSession = retrieveVerificationSession(registrationServiceSession);

    if (registrationServiceSession.verified()) {
      throw new ClientErrorException(
          Response.status(Response.Status.CONFLICT)
              .entity(buildResponse(registrationServiceSession, verificationSession))
              .build());
    }

    if (!verificationSession.allowedToRequestCode()) {
      final Response.Status status = verificationSession.requestedInformation().isEmpty()
          ? Response.Status.TOO_MANY_REQUESTS
          : Response.Status.CONFLICT;

      throw new ClientErrorException(
          Response.status(status)
              .entity(buildResponse(registrationServiceSession, verificationSession))
              .build());
    }

    final MessageTransport messageTransport = verificationCodeRequest.transport().toMessageTransport();

    final ClientType clientType = switch (verificationCodeRequest.client()) {
      case "ios" -> ClientType.IOS;
      case "android-2021-03" -> ClientType.ANDROID_WITH_FCM;
      default -> {
        if (StringUtils.startsWithIgnoreCase(verificationCodeRequest.client(), "android")) {
          yield ClientType.ANDROID_WITHOUT_FCM;
        }
        yield ClientType.UNKNOWN;
      }
    };

    final String senderOverride = switch (messageTransport) {
      case SMS -> verificationSession.smsSenderOverride();
      case VOICE -> verificationSession.voiceSenderOverride();
    };

    final RegistrationServiceSession resultSession;
    try {
      resultSession = registrationServiceClient.sendVerificationCode(registrationServiceSession.id(),
          messageTransport,
          clientType,
          acceptLanguage.orElse(null),
          senderOverride,
          REGISTRATION_RPC_TIMEOUT).join();
    } catch (final CancellationException e) {
      throw new ServerErrorException("registration service unavailable", Response.Status.SERVICE_UNAVAILABLE);
    } catch (final CompletionException e) {
      final Throwable unwrappedException = ExceptionUtils.unwrap(e);
      if (unwrappedException instanceof RateLimitExceededException rateLimitExceededException) {
        if (rateLimitExceededException instanceof VerificationSessionRateLimitExceededException ve) {
          final Response response = buildResponseForRateLimitExceeded(verificationSession, ve.getRegistrationSession(),
              ve.getRetryDuration());
          throw new ClientErrorException(response);
        }

        throw new RateLimitExceededException(rateLimitExceededException.getRetryDuration().orElse(null));
      } else if (unwrappedException instanceof RegistrationServiceException registrationServiceException) {

        throw registrationServiceException.getRegistrationSession()
            .map(s -> buildResponse(s, verificationSession))
            .map(verificationSessionResponse -> {
              final Response response = registrationServiceException instanceof TransportNotAllowedException
                  ? Response.status(418).entity(verificationSessionResponse).build()
                  : Response.status(Response.Status.CONFLICT).entity(verificationSessionResponse).build();

              return new ClientErrorException(response);
            })
            .orElseGet(NotFoundException::new);

      } else if (unwrappedException instanceof RegistrationFraudException) {
        if (dynamicConfigurationManager.getConfiguration().getRegistrationConfiguration().squashDeclinedAttemptErrors()) {
          return buildResponse(registrationServiceSession, verificationSession);
        } else {
          throw unwrappedException.getCause();
        }
      } else if (unwrappedException instanceof RegistrationServiceSenderException) {
        throw unwrappedException;
      } else {
        logger.error("Registration service failure", unwrappedException);
        throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
      }
    }

    Metrics.counter(CODE_REQUESTED_COUNTER_NAME, Tags.of(
            UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(registrationServiceSession.number())),
            Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(registrationServiceSession.number())),
            Tag.of(VERIFICATION_TRANSPORT_TAG_NAME, verificationCodeRequest.transport().toString())))
        .increment();

    return buildResponse(resultSession, verificationSession);
  }

  @PUT
  @Path("/session/{sessionId}/code")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Submit a verification code",
      description = """
          Submits a verification code received via SMS or voice for verification
          """)
  @ApiResponse(responseCode = "200", description = """
      The request to check a verification code was processed (though the submitted code may not be the correct code);
      the session metadata will indicate whether the submitted code was correct
      """, useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid session ID or verification  code")
  @ApiResponse(responseCode = "404", description = "Session with the specified ID could not be found")
  @ApiResponse(responseCode = "409", description = "The session is already verified or no code has been requested yet for this session",
      content = @Content(schema = @Schema(implementation = VerificationSessionResponse.class)))
  @ApiResponse(responseCode = "429", description = """
      Too many attempts; the caller is not permitted to submit a verification code at this time and may need to wait 
      before trying again; if the session metadata does not specify a time at which the caller may try again, then the 
      caller has exhausted their permitted attempts and must create a new verification session.
      """,
      content = @Content(schema = @Schema(implementation = VerificationSessionResponse.class)),
      headers = @Header(
          name = "Retry-After",
          description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed",
          schema = @Schema(implementation = Integer.class)))
  public VerificationSessionResponse verifyCode(@PathParam("sessionId") final String encodedSessionId,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @NotNull @Valid final SubmitVerificationCodeRequest submitVerificationCodeRequest)
      throws RateLimitExceededException {

    final RegistrationServiceSession registrationServiceSession = retrieveRegistrationServiceSession(encodedSessionId);
    final VerificationSession verificationSession = retrieveVerificationSession(registrationServiceSession);

    if (registrationServiceSession.verified()) {
      final VerificationSessionResponse verificationSessionResponse = buildResponse(registrationServiceSession,
          verificationSession);

      throw new ClientErrorException(
          Response.status(Response.Status.CONFLICT).entity(verificationSessionResponse).build());
    }

    final RegistrationServiceSession resultSession;
    try {
      resultSession = registrationServiceClient.checkVerificationCode(registrationServiceSession.id(),
              submitVerificationCodeRequest.code(),
              REGISTRATION_RPC_TIMEOUT)
          .join();
    } catch (final CancellationException e) {
      logger.warn("Unexpected cancellation from registration service", e);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE);
    } catch (final CompletionException e) {
      final Throwable unwrappedException = ExceptionUtils.unwrap(e);
      if (unwrappedException instanceof RateLimitExceededException rateLimitExceededException) {

        if (rateLimitExceededException instanceof VerificationSessionRateLimitExceededException ve) {
          final Response response = buildResponseForRateLimitExceeded(verificationSession, ve.getRegistrationSession(),
              ve.getRetryDuration());
          throw new ClientErrorException(response);
        }

        throw new RateLimitExceededException(rateLimitExceededException.getRetryDuration().orElse(null));

      } else if (unwrappedException instanceof RegistrationServiceException registrationServiceException) {

        throw registrationServiceException.getRegistrationSession()
            .map(s -> buildResponse(s, verificationSession))
            .map(verificationSessionResponse -> new ClientErrorException(
                Response.status(Response.Status.CONFLICT).entity(verificationSessionResponse).build()))
            .orElseGet(NotFoundException::new);

      } else {
        logger.error("Registration service failure", unwrappedException);
        throw new ServerErrorException(Response.Status.INTERNAL_SERVER_ERROR);
      }
    }

    if (resultSession.verified()) {
      registrationRecoveryPasswordsManager.remove(phoneNumberIdentifiers.getPhoneNumberIdentifier(registrationServiceSession.number()).join());
    }

    Metrics.counter(VERIFIED_COUNTER_NAME, Tags.of(
            UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(registrationServiceSession.number())),
            Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(registrationServiceSession.number())),
            Tag.of(SUCCESS_TAG_NAME, Boolean.toString(resultSession.verified()))))
        .increment();

    return buildResponse(resultSession, verificationSession);
  }

  private Response buildResponseForRateLimitExceeded(final VerificationSession verificationSession,
      final RegistrationServiceSession registrationServiceSession,
      final Optional<Duration> retryDuration) {

    final Response.ResponseBuilder responseBuilder = Response.status(Response.Status.TOO_MANY_REQUESTS)
        .entity(buildResponse(registrationServiceSession, verificationSession));

    retryDuration
        .filter(d -> !d.isNegative())
        .ifPresent(d -> responseBuilder.header(HttpHeaders.RETRY_AFTER, d.toSeconds()));

    return responseBuilder.build();
  }

  /**
   * @throws ClientErrorException with {@code 422} status if the ID cannot be decoded
   * @throws NotFoundException    if the ID cannot be found
   */
  private RegistrationServiceSession retrieveRegistrationServiceSession(final String encodedSessionId) {
    final byte[] sessionId;

    try {
      sessionId = decodeSessionId(encodedSessionId);
    } catch (final IllegalArgumentException e) {
      throw new ClientErrorException("Malformed session ID", HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    try {
      final RegistrationServiceSession registrationServiceSession = registrationServiceClient.getSession(sessionId,
              REGISTRATION_RPC_TIMEOUT).join()
          .orElseThrow(NotFoundException::new);

      if (registrationServiceSession.verified()) {
        registrationRecoveryPasswordsManager.remove(phoneNumberIdentifiers.getPhoneNumberIdentifier(registrationServiceSession.number()).join());
      }

      return registrationServiceSession;

    } catch (final CompletionException | CancellationException e) {
      final Throwable unwrapped = ExceptionUtils.unwrap(e);

      if (unwrapped instanceof StatusRuntimeException grpcRuntimeException) {
        if (grpcRuntimeException.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
          throw new BadRequestException();
        }
      }
      logger.error("Registration service failure", e);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE, e);
    }
  }

  /**
   * @throws NotFoundException if the session is has no record
   */
  private VerificationSession retrieveVerificationSession(final RegistrationServiceSession registrationServiceSession) {

    return verificationSessionManager.findForId(registrationServiceSession.encodedSessionId())
        .orTimeout(5, TimeUnit.SECONDS)
        .join().orElseThrow(NotFoundException::new);
  }

  /**
   * @throws ClientErrorException with {@code 422} status if the only one of token and type are present
   */
  private Pair<String, PushNotification.TokenType> validateAndExtractPushToken(
      final UpdateVerificationSessionRequest request) {

    final String pushToken;
    final PushNotification.TokenType pushTokenType;
    if (Objects.isNull(request.pushToken())
        != Objects.isNull(request.pushTokenType())) {
      throw new WebApplicationException("must specify both pushToken and pushTokenType or neither",
          HttpStatus.SC_UNPROCESSABLE_ENTITY);
    } else {
      pushToken = request.pushToken();
      pushTokenType = pushToken == null
          ? null
          : request.pushTokenType().toTokenType();
    }

    return new Pair<>(pushToken, pushTokenType);
  }

  private VerificationSessionResponse buildResponse(final RegistrationServiceSession registrationServiceSession,
      final VerificationSession verificationSession) {
    return new VerificationSessionResponse(registrationServiceSession.encodedSessionId(),
        registrationServiceSession.nextSms(),
        registrationServiceSession.nextVoiceCall(), registrationServiceSession.nextVerificationAttempt(),
        verificationSession.allowedToRequestCode(), verificationSession.requestedInformation(),
        registrationServiceSession.verified());
  }

  public static byte[] decodeSessionId(final String sessionId) {
    return Base64.getUrlDecoder().decode(sessionId);
  }

  private static String generatePushChallenge() {
    final byte[] challenge = new byte[16];
    RANDOM.nextBytes(challenge);

    return HexFormat.of().formatHex(challenge);
  }

}
