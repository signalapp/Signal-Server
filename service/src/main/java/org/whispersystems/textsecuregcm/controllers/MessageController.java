/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.Auth;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage.Recipient;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.util.Pair;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.groupsend.GroupSendDerivedKeyPair;
import org.signal.libsignal.zkgroup.groupsend.GroupSendFullToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CombinedUnidentifiedSenderAccessKeys;
import org.whispersystems.textsecuregcm.auth.GroupSendTokenHeader;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountMismatchedDevices;
import org.whispersystems.textsecuregcm.entities.AccountStaleDevices;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope.Type;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;
import org.whispersystems.textsecuregcm.entities.SendMultiRecipientMessageResponse;
import org.whispersystems.textsecuregcm.entities.SpamReport;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.MessageDeliveryLoopMonitor;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.util.DestinationDeviceValidator;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;
import org.whispersystems.websocket.Stories;
import org.whispersystems.websocket.auth.ReadOnly;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuples;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/messages")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Messages")
public class MessageController {


  private record MultiRecipientDeliveryData(
      ServiceIdentifier serviceIdentifier,
      Account account,
      Recipient recipient,
      Map<Byte, Short> deviceIdToRegistrationId) {
  }

  private static final Logger logger = LoggerFactory.getLogger(MessageController.class);

  private final RateLimiters rateLimiters;
  private final CardinalityEstimator messageByteLimitEstimator;
  private final MessageSender messageSender;
  private final ReceiptSender receiptSender;
  private final AccountsManager accountsManager;
  private final MessagesManager messagesManager;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final ReportMessageManager reportMessageManager;
  private final ExecutorService multiRecipientMessageExecutor;
  private final Scheduler messageDeliveryScheduler;
  private final ClientReleaseManager clientReleaseManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final ServerSecretParams serverSecretParams;
  private final SpamChecker spamChecker;
  private final MessageMetrics messageMetrics;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;
  private final Clock clock;

  private static final int MAX_FETCH_ACCOUNT_CONCURRENCY = 8;

  private static final CompletableFuture<?>[] EMPTY_FUTURE_ARRAY = new CompletableFuture<?>[0];

  private static final String REJECT_OVERSIZE_MESSAGE_COUNTER = name(MessageController.class, "rejectOversizeMessage");
  private static final String SENT_MESSAGE_COUNTER_NAME = name(MessageController.class, "sentMessages");
  private static final String CONTENT_SIZE_DISTRIBUTION_NAME = MetricsUtil.name(MessageController.class, "messageContentSize");
  private static final String OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME = name(MessageController.class, "outgoingMessageListSizeBytes");
  private static final String RATE_LIMITED_MESSAGE_COUNTER_NAME = name(MessageController.class, "rateLimitedMessage");

  private static final String REJECT_INVALID_ENVELOPE_TYPE = name(MessageController.class, "rejectInvalidEnvelopeType");
  private static final String SEND_MESSAGE_LATENCY_TIMER_NAME = MetricsUtil.name(MessageController.class, "sendMessageLatency");

  private static final String EPHEMERAL_TAG_NAME = "ephemeral";
  private static final String SENDER_TYPE_TAG_NAME = "senderType";
  private static final String AUTH_TYPE_TAG_NAME = "authType";
  private static final String SENDER_COUNTRY_TAG_NAME = "senderCountry";
  private static final String RATE_LIMIT_REASON_TAG_NAME = "rateLimitReason";
  private static final String ENVELOPE_TYPE_TAG_NAME = "envelopeType";
  private static final String IDENTITY_TYPE_TAG_NAME = "identityType";
  private static final String ENDPOINT_TYPE_TAG_NAME = "endpoint";

  private static final String SENDER_TYPE_IDENTIFIED = "identified";
  private static final String SENDER_TYPE_UNIDENTIFIED = "unidentified";
  private static final String SENDER_TYPE_SELF = "self";

  private static final String AUTH_TYPE_IDENTIFIED = "identified";
  private static final String AUTH_TYPE_ACCESS_KEY = "accessKey";
  private static final String AUTH_TYPE_GROUP_SEND_TOKEN = "groupSendToken";
  private static final String AUTH_TYPE_STORY = "story";

  private static final String ENDPOINT_TYPE_SINGLE = "single";
  private static final String ENDPOINT_TYPE_MULTI = "multi";

  @VisibleForTesting
  static final long MAX_MESSAGE_SIZE = DataSize.kibibytes(256).toBytes();

  private static final Duration NOTIFY_FOR_REMAINING_MESSAGES_DELAY = Duration.ofMinutes(1);

  public MessageController(
      RateLimiters rateLimiters,
      CardinalityEstimator messageByteLimitEstimator,
      MessageSender messageSender,
      ReceiptSender receiptSender,
      AccountsManager accountsManager,
      MessagesManager messagesManager,
      PushNotificationManager pushNotificationManager,
      PushNotificationScheduler pushNotificationScheduler,
      ReportMessageManager reportMessageManager,
      @Nonnull ExecutorService multiRecipientMessageExecutor,
      Scheduler messageDeliveryScheduler,
      final ClientReleaseManager clientReleaseManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ServerSecretParams serverSecretParams,
      final SpamChecker spamChecker,
      final MessageMetrics messageMetrics,
      final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor,
      final Clock clock) {
    this.rateLimiters = rateLimiters;
    this.messageByteLimitEstimator = messageByteLimitEstimator;
    this.messageSender = messageSender;
    this.receiptSender = receiptSender;
    this.accountsManager = accountsManager;
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.reportMessageManager = reportMessageManager;
    this.multiRecipientMessageExecutor = Objects.requireNonNull(multiRecipientMessageExecutor);
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.clientReleaseManager = clientReleaseManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.serverSecretParams = serverSecretParams;
    this.spamChecker = spamChecker;
    this.messageMetrics = messageMetrics;
    this.messageDeliveryLoopMonitor = messageDeliveryLoopMonitor;
    this.clock = clock;
  }

  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  @Operation(
      summary = "Send a message",
      description = """
          Deliver a message to a single recipient. May be authenticated or unauthenticated; if unauthenticated,
          an unidentifed-access key or group-send endorsement token must be provided, unless the message is a story.
          """)
  @ApiResponse(responseCode="200", description="Message was successfully sent", useReturnTypeSchema=true)
  @ApiResponse(
      responseCode="401",
      description="The message is not a story and the authorization, unauthorized access key, or group send endorsement token is missing or incorrect")
  @ApiResponse(
      responseCode="404",
      description="The message is not a story and some the recipient service ID does not correspond to a registered Signal user")
  @ApiResponse(
      responseCode = "409", description = "Incorrect set of devices supplied for recipient",
      content = @Content(schema = @Schema(implementation = AccountMismatchedDevices[].class)))
  @ApiResponse(
      responseCode = "410", description = "Mismatched registration ids supplied for some recipient devices",
      content = @Content(schema = @Schema(implementation = AccountStaleDevices[].class)))
  @ApiResponse(
      responseCode="428",
      description="The sender should complete a challenge before proceeding")
  public Response sendMessage(@ReadOnly @Auth final Optional<AuthenticatedDevice> source,
      @Parameter(description="The recipient's unidentified access key")
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) final Optional<Anonymous> accessKey,

      @Parameter(description="A group send endorsement token covering the recipient. Must not be combined with `Unidentified-Access-Key` or set on a story message.")
      @HeaderParam(HeaderUtils.GROUP_SEND_TOKEN)
      @Nullable final GroupSendTokenHeader groupSendToken,

      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description="If true, deliver the message only to recipients that are online when it is sent")
      @PathParam("destination") final ServiceIdentifier destinationIdentifier,

      @Parameter(description="If true, the message is a story; access tokens are not checked and sending to nonexistent recipients is permitted")
      @QueryParam("story") final boolean isStory,

      @Parameter(description="The encrypted message payloads for each recipient device")
      @NotNull @Valid final IncomingMessageList messages,

      @Context final ContainerRequestContext context) throws RateLimitExceededException {

    if (source.isEmpty() && accessKey.isEmpty() && groupSendToken == null && !isStory) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }

    if (groupSendToken != null) {
      if (source.isPresent() || accessKey.isPresent()) {
        throw new BadRequestException(
            "Group send endorsement tokens should not be combined with other authentication");
      } else if (isStory) {
        throw new BadRequestException("Group send endorsement tokens should not be sent for story messages");
      }
    }

    final String senderType = source.map(
            s -> s.getAccount().isIdentifiedBy(destinationIdentifier) ? SENDER_TYPE_SELF : SENDER_TYPE_IDENTIFIED)
        .orElse(SENDER_TYPE_UNIDENTIFIED);

    final Sample sample = Timer.start();
    try {
      final boolean isSyncMessage = senderType.equals(SENDER_TYPE_SELF);

      if (isSyncMessage && destinationIdentifier.identityType() == IdentityType.PNI) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }

      final Optional<Account> destination;
      if (!isSyncMessage) {
        destination = accountsManager.getByServiceIdentifier(destinationIdentifier);
      } else {
        destination = source.map(AuthenticatedDevice::getAccount);
      }

      final SpamChecker.SpamCheckResult spamCheck = spamChecker.checkForSpam(
          context, source, destination, Optional.of(destinationIdentifier));
      final Optional<byte[]> reportSpamToken;
      switch (spamCheck) {
        case final SpamChecker.Spam spam: return spam.response();
        case final SpamChecker.NotSpam notSpam: reportSpamToken = notSpam.token();
      }

      int totalContentLength = 0;

      for (final IncomingMessage message : messages.messages()) {
        int contentLength = 0;

        if (StringUtils.isNotEmpty(message.content())) {
          contentLength += message.content().length();
        }

        validateContentLength(contentLength, false, userAgent);
        validateEnvelopeType(message.type(), userAgent);

        totalContentLength += contentLength;
      }

      try {
        rateLimiters.getInboundMessageBytes().validate(destinationIdentifier.uuid(), totalContentLength);
      } catch (final RateLimitExceededException e) {
        if (dynamicConfigurationManager.getConfiguration().getInboundMessageByteLimitConfiguration().enforceInboundLimit()) {
          messageByteLimitEstimator.add(destinationIdentifier.uuid().toString());
          throw e;
        }
      }

      try {
        if (isStory) {
          // Stories will be checked by the client; we bypass access checks here for stories.
        } else if (groupSendToken != null) {
          checkGroupSendToken(List.of(destinationIdentifier.toLibsignal()), groupSendToken);
          if (destination.isEmpty()) {
            throw new NotFoundException();
          }
        } else {
          OptionalAccess.verify(source.map(AuthenticatedDevice::getAccount), accessKey, destination,
              destinationIdentifier);
        }

        final boolean needsSync = !isSyncMessage && source.isPresent() && source.get().getAccount().getDevices().size() > 1;

        // We return 200 when stories are sent to a non-existent account. Since story sends bypass OptionalAccess.verify
        // we leak information about whether a destination UUID exists if we return any other code (e.g. 404) from
        // these requests.
        if (isStory && destination.isEmpty()) {
          return Response.ok(new SendMessageResponse(needsSync)).build();
        }

        // if destination is empty we would either throw an exception in OptionalAccess.verify when isStory is false
        // or else return a 200 response when isStory is true.
        assert destination.isPresent();

        if (source.isPresent() && !isSyncMessage) {
          checkMessageRateLimit(source.get(), destination.get(), userAgent);
        }

        if (isStory) {
          rateLimiters.getStoriesLimiter().validate(destination.get().getUuid());
        }

        final Set<Byte> excludedDeviceIds;

        if (isSyncMessage) {
          excludedDeviceIds = Set.of(source.get().getAuthenticatedDevice().getId());
        } else {
          excludedDeviceIds = Collections.emptySet();
        }

        DestinationDeviceValidator.validateCompleteDeviceList(destination.get(),
            messages.messages().stream().map(IncomingMessage::destinationDeviceId).collect(Collectors.toSet()),
            excludedDeviceIds);

        DestinationDeviceValidator.validateRegistrationIds(destination.get(),
            messages.messages(),
            IncomingMessage::destinationDeviceId,
            IncomingMessage::destinationRegistrationId,
            destination.get().getPhoneNumberIdentifier().equals(destinationIdentifier.uuid()));

        final String authType;
        if (SENDER_TYPE_IDENTIFIED.equals(senderType)) {
          authType = AUTH_TYPE_IDENTIFIED;
        } else if (isStory) {
          authType = AUTH_TYPE_STORY;
        } else if (groupSendToken != null) {
          authType = AUTH_TYPE_GROUP_SEND_TOKEN;
        } else {
          authType = AUTH_TYPE_ACCESS_KEY;
        }

        final List<Tag> tags = List.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(ENDPOINT_TYPE_TAG_NAME, ENDPOINT_TYPE_SINGLE),
            Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(messages.online())),
            Tag.of(SENDER_TYPE_TAG_NAME, senderType),
            Tag.of(AUTH_TYPE_TAG_NAME, authType),
            Tag.of(IDENTITY_TYPE_TAG_NAME, destinationIdentifier.identityType().name()));

        for (final IncomingMessage incomingMessage : messages.messages()) {
          destination.get().getDevice(incomingMessage.destinationDeviceId())
              .ifPresent(destinationDevice -> {
                Metrics.counter(SENT_MESSAGE_COUNTER_NAME, tags).increment();
                sendIndividualMessage(
                    source,
                    destination.get(),
                    destinationDevice,
                    destinationIdentifier,
                    messages.timestamp(),
                    messages.online(),
                    isStory,
                    messages.urgent(),
                    incomingMessage,
                    userAgent,
                    reportSpamToken);
              });
        }

        return Response.ok(new SendMessageResponse(needsSync)).build();
      } catch (final MismatchedDevicesException e) {
        throw new WebApplicationException(Response.status(409)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(new MismatchedDevices(e.getMissingDevices(),
                e.getExtraDevices()))
            .build());
      } catch (final StaleDevicesException e) {
        throw new WebApplicationException(Response.status(410)
            .type(MediaType.APPLICATION_JSON)
            .entity(new StaleDevices(e.getStaleDevices()))
            .build());
      }
    } finally {
      sample.stop(Timer.builder(SEND_MESSAGE_LATENCY_TIMER_NAME)
          .tags(SENDER_TYPE_TAG_NAME, senderType)
          .publishPercentileHistogram(true)
          .register(Metrics.globalRegistry));
    }
  }


  /**
   * Build mapping of service IDs to resolved accounts and device/registration IDs
   */
  private Map<ServiceIdentifier, MultiRecipientDeliveryData> buildRecipientMap(
      SealedSenderMultiRecipientMessage multiRecipientMessage, boolean isStory) {
    return Flux.fromIterable(multiRecipientMessage.getRecipients().entrySet())
        .switchIfEmpty(Flux.error(BadRequestException::new))
        .map(e -> Tuples.of(ServiceIdentifier.fromLibsignal(e.getKey()), e.getValue()))
        .flatMap(
            t -> Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(t.getT1()))
                .flatMap(Mono::justOrEmpty)
                .switchIfEmpty(isStory ? Mono.empty() : Mono.error(NotFoundException::new))
                .map(
                    account ->
                        new MultiRecipientDeliveryData(
                            t.getT1(),
                            account,
                            t.getT2(),
                            t.getT2().getDevicesAndRegistrationIds().collect(
                                Collectors.toMap(Pair<Byte, Short>::first, Pair<Byte, Short>::second))))
                // IllegalStateException is thrown by Collectors#toMap when we have multiple entries for the same device
                .onErrorMap(e -> e instanceof IllegalStateException ? new BadRequestException() : e),
            MAX_FETCH_ACCOUNT_CONCURRENCY)
        .collectMap(MultiRecipientDeliveryData::serviceIdentifier)
        .block();
  }

  @Timed
  @Path("/multi_recipient")
  @PUT
  @Consumes(MultiRecipientMessageProvider.MEDIA_TYPE)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Send multi-recipient sealed-sender message",
      description = """
          Deliver a common-payload message to multiple recipients.
          An unidentifed-access key for all recipients must be provided, unless the message is a story.
          """)
  @ApiResponse(responseCode="200", description="Message was successfully sent to all recipients", useReturnTypeSchema=true)
  @ApiResponse(responseCode="400", description="The envelope specified delivery to the same recipient device multiple times")
  @ApiResponse(
      responseCode="401",
      description="The message is not a story and the unauthorized access key or group send endorsement token is missing or incorrect")
  @ApiResponse(
      responseCode="404",
      description="The message is not a story and some of the recipient service IDs do not correspond to registered Signal users")
  @ApiResponse(
      responseCode = "409", description = "Incorrect set of devices supplied for some recipients",
      content = @Content(schema = @Schema(implementation = AccountMismatchedDevices[].class)))
  @ApiResponse(
      responseCode = "410", description = "Mismatched registration ids supplied for some recipient devices",
      content = @Content(schema = @Schema(implementation = AccountStaleDevices[].class)))
  public Response sendMultiRecipientMessage(
      @Deprecated
      @Parameter(description="The bitwise xor of the unidentified access keys for every recipient of the message. Will be replaced with group send endorsements")
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) @Nullable CombinedUnidentifiedSenderAccessKeys accessKeys,

      @Parameter(description="A group send endorsement token covering recipients of this message. Must not be combined with `Unidentified-Access-Key` or set on a story message.")
      @HeaderParam(HeaderUtils.GROUP_SEND_TOKEN)
      @Nullable GroupSendTokenHeader groupSendToken,

      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,

      @Parameter(description="If true, deliver the message only to recipients that are online when it is sent")
      @QueryParam("online") boolean online,

      @Parameter(description="The sender's timestamp for the envelope")
      @QueryParam("ts") long timestamp,

      @Parameter(description="If true, this message should cause push notifications to be sent to recipients")
      @QueryParam("urgent") @DefaultValue("true") final boolean isUrgent,

      @Parameter(description="If true, the message is a story; access tokens are not checked and sending to nonexistent recipients is permitted")
      @QueryParam("story") boolean isStory,
      @Parameter(description="The sealed-sender multi-recipient message payload as serialized by libsignal")
      @NotNull SealedSenderMultiRecipientMessage multiRecipientMessage,

      @Context ContainerRequestContext context) throws RateLimitExceededException {

    final SpamChecker.SpamCheckResult spamCheck = spamChecker.checkForSpam(context, Optional.empty(), Optional.empty(), Optional.empty());
    if (spamCheck instanceof final SpamChecker.Spam spam) {
      return spam.response();
    }

    if (groupSendToken == null && accessKeys == null && !isStory) {
      throw new NotAuthorizedException("A group send endorsement token or unidentified access key is required for non-story messages");
    }
    if (groupSendToken != null) {
      if (accessKeys != null) {
        throw new BadRequestException("Only one of group send endorsement token and unidentified access key may be provided");
      } else if (isStory) {
        throw new BadRequestException("Stories should not provide a group send endorsement token");
      }
    }

    if (groupSendToken != null) {
      // Group send endorsements are checked before we even attempt to resolve any accounts, since
      // the lists of service IDs in the envelope are all that we need to check against
      checkGroupSendToken(
          multiRecipientMessage.getRecipients().keySet(), groupSendToken);
    }

    final Map<ServiceIdentifier, MultiRecipientDeliveryData> recipients = buildRecipientMap(multiRecipientMessage, isStory);

    // Access keys are checked against the UAK in the resolved accounts, so we have to check after resolving accounts above.
    // Group send endorsements are checked earlier; for stories, we don't check permissions at all because only clients check them
    if (groupSendToken == null && !isStory) {
      checkAccessKeys(accessKeys, recipients.values());
    }
    // We might filter out all the recipients of a story (if none exist).
    // In this case there is no error so we should just return 200 now.
    if (isStory) {
      if (recipients.isEmpty()) {
        return Response.ok(new SendMultiRecipientMessageResponse(List.of())).build();
      }

      try {
        CompletableFuture.allOf(recipients.values()
                .stream()
                .map(recipient -> recipient.account().getUuid())
                .map(accountIdentifier ->
                    rateLimiters.getStoriesLimiter().validateAsync(accountIdentifier).toCompletableFuture())
                .toList()
                .toArray(EMPTY_FUTURE_ARRAY))
            .join();
      } catch (final Exception e) {
        if (ExceptionUtils.unwrap(e) instanceof RateLimitExceededException rateLimitExceededException) {
          throw rateLimitExceededException;
        } else {
          throw ExceptionUtils.wrap(e);
        }
      }
    }

    Collection<AccountMismatchedDevices> accountMismatchedDevices = new ArrayList<>();
    Collection<AccountStaleDevices> accountStaleDevices = new ArrayList<>();
    recipients.values().forEach(recipient -> {
      final Account account = recipient.account();

      try {
        DestinationDeviceValidator.validateCompleteDeviceList(account, recipient.deviceIdToRegistrationId().keySet(),
            Collections.emptySet());

        DestinationDeviceValidator.validateRegistrationIds(
            account,
            recipient.deviceIdToRegistrationId().entrySet(),
            Map.Entry<Byte, Short>::getKey,
            e -> Integer.valueOf(e.getValue()),
            recipient.serviceIdentifier().identityType() == IdentityType.PNI);
      } catch (MismatchedDevicesException e) {
        accountMismatchedDevices.add(
            new AccountMismatchedDevices(
                recipient.serviceIdentifier(),
                new MismatchedDevices(e.getMissingDevices(), e.getExtraDevices())));
      } catch (StaleDevicesException e) {
        accountStaleDevices.add(
            new AccountStaleDevices(recipient.serviceIdentifier(), new StaleDevices(e.getStaleDevices())));
      }
    });
    if (!accountMismatchedDevices.isEmpty()) {
      return Response
          .status(409)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(accountMismatchedDevices)
          .build();
    }
    if (!accountStaleDevices.isEmpty()) {
      return Response
          .status(410)
          .type(MediaType.APPLICATION_JSON)
          .entity(accountStaleDevices)
          .build();
    }

    final String authType;
    if (isStory) {
      authType = AUTH_TYPE_STORY;
    } else if (groupSendToken != null) {
      authType = AUTH_TYPE_GROUP_SEND_TOKEN;
    } else {
      authType = AUTH_TYPE_ACCESS_KEY;
    }

    try {
      final byte[] sharedMrmKey = messagesManager.insertSharedMultiRecipientMessagePayload(multiRecipientMessage);

      CompletableFuture.allOf(
              recipients.values().stream()
                  .flatMap(recipientData -> {
                    final Counter sentMessageCounter = Metrics.counter(SENT_MESSAGE_COUNTER_NAME, Tags.of(
                        UserAgentTagUtil.getPlatformTag(userAgent),
                        Tag.of(ENDPOINT_TYPE_TAG_NAME, ENDPOINT_TYPE_MULTI),
                        Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(online)),
                        Tag.of(SENDER_TYPE_TAG_NAME, SENDER_TYPE_UNIDENTIFIED),
                        Tag.of(AUTH_TYPE_TAG_NAME, authType),
                        Tag.of(IDENTITY_TYPE_TAG_NAME, recipientData.serviceIdentifier().identityType().name())));

                    validateContentLength(multiRecipientMessage.messageSizeForRecipient(recipientData.recipient()), true, userAgent);

                    return recipientData.deviceIdToRegistrationId().keySet().stream().map(
                        deviceId -> CompletableFuture.runAsync(
                            () -> {
                              final Account destinationAccount = recipientData.account();
                              final byte[] payload = multiRecipientMessage.messageForRecipient(recipientData.recipient());

                              // we asserted this must exist in validateCompleteDeviceList
                              final Device destinationDevice = destinationAccount.getDevice(deviceId).orElseThrow();

                              sentMessageCounter.increment();
                              sendCommonPayloadMessage(
                                  destinationAccount, destinationDevice, recipientData.serviceIdentifier(), timestamp,
                                  online, isStory, isUrgent, payload, sharedMrmKey);
                            },
                            multiRecipientMessageExecutor));
                  })
                  .toArray(CompletableFuture[]::new))
          .get();
    } catch (InterruptedException e) {
      logger.error("interrupted while delivering multi-recipient messages", e);
      throw new InternalServerErrorException("interrupted during delivery");
    } catch (CancellationException e) {
      logger.error("cancelled while delivering multi-recipient messages", e);
      throw new InternalServerErrorException("delivery cancelled");
    } catch (ExecutionException e) {
      logger.error("partial failure while delivering multi-recipient messages", e.getCause());
      throw new InternalServerErrorException("failure during delivery");
    }
    return Response.ok(new SendMultiRecipientMessageResponse(Collections.emptyList())).build();
  }

  private void checkGroupSendToken(
      final Collection<ServiceId> recipients,
      final @NotNull GroupSendTokenHeader groupSendToken) {
    try {
      final GroupSendFullToken token = groupSendToken.token();
      token.verify(recipients, clock.instant(), GroupSendDerivedKeyPair.forExpiration(token.getExpiration(), serverSecretParams));
    } catch (VerificationFailedException e) {
      throw new NotAuthorizedException(e);
    }
  }

  private void checkAccessKeys(
      final @NotNull CombinedUnidentifiedSenderAccessKeys accessKeys,
      final Collection<MultiRecipientDeliveryData> destinations) {
    final int keyLength = UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH;

    if (destinations.stream()
        .anyMatch(destination -> IdentityType.PNI.equals(destination.serviceIdentifier.identityType()))) {
      throw new WebApplicationException("Multi-recipient messages must be addressed to ACI service IDs",
          Status.UNAUTHORIZED);
    }

    final byte[] combinedUnidentifiedAccessKeys = destinations.stream()
        .map(MultiRecipientDeliveryData::account)
        .filter(Predicate.not(Account::isUnrestrictedUnidentifiedAccess))
        .map(account ->
            account.getUnidentifiedAccessKey()
                .filter(b -> b.length == keyLength)
                .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED)))
        .reduce(new byte[keyLength],
            (a, b) -> {
              final byte[] xor = new byte[keyLength];
              IntStream.range(0, keyLength).forEach(i -> xor[i] = (byte) (a[i] ^ b[i]));
              return xor;
            });
    if (!MessageDigest.isEqual(combinedUnidentifiedAccessKeys, accessKeys.getAccessKeys())) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<OutgoingMessageEntityList> getPendingMessages(@ReadOnly @Auth AuthenticatedDevice auth,
      @HeaderParam(Stories.X_SIGNAL_RECEIVE_STORIES) String receiveStoriesHeader,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent) {

    boolean shouldReceiveStories = Stories.parseReceiveStoriesHeader(receiveStoriesHeader);

    pushNotificationManager.handleMessagesRetrieved(auth.getAccount(), auth.getAuthenticatedDevice(), userAgent);

    return messagesManager.getMessagesForDevice(
            auth.getAccount().getUuid(),
            auth.getAuthenticatedDevice(),
            false)
        .map(messagesAndHasMore -> {
          Stream<Envelope> envelopes = messagesAndHasMore.first().stream();
          if (!shouldReceiveStories) {
            envelopes = envelopes.filter(e -> !e.getStory());
          }

          final OutgoingMessageEntityList messages = new OutgoingMessageEntityList(envelopes
              .map(OutgoingMessageEntity::fromEnvelope)
              .peek(outgoingMessageEntity -> {
                messageMetrics.measureAccountOutgoingMessageUuidMismatches(auth.getAccount(), outgoingMessageEntity);
                messageMetrics.measureOutgoingMessageLatency(outgoingMessageEntity.serverTimestamp(),
                    "rest",
                    auth.getAuthenticatedDevice().isPrimary(),
                    userAgent,
                    clientReleaseManager);
              })
              .collect(Collectors.toList()),
              messagesAndHasMore.second());

          Metrics.summary(OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent)))
              .record(estimateMessageListSizeBytes(messages));

          if (!messages.messages().isEmpty()) {
            messageDeliveryLoopMonitor.recordDeliveryAttempt(auth.getAccount().getIdentifier(IdentityType.ACI),
                auth.getAuthenticatedDevice().getId(),
                messages.messages().getFirst().guid(),
                userAgent,
                "rest");
          }

          if (messagesAndHasMore.second()) {
            pushNotificationScheduler.scheduleDelayedNotification(auth.getAccount(), auth.getAuthenticatedDevice(), NOTIFY_FOR_REMAINING_MESSAGES_DELAY);
          }

          return messages;
        })
        .timeout(Duration.ofSeconds(5))
        .subscribeOn(messageDeliveryScheduler)
        .toFuture();
  }

  private static long estimateMessageListSizeBytes(final OutgoingMessageEntityList messageList) {
    long size = 0;

    for (final OutgoingMessageEntity message : messageList.messages()) {
      size += message.content() == null ? 0 : message.content().length;
      size += message.sourceUuid() == null ? 0 : 36;
    }

    return size;
  }

  @Timed
  @DELETE
  @Path("/uuid/{uuid}")
  public CompletableFuture<Response> removePendingMessage(@ReadOnly @Auth AuthenticatedDevice auth, @PathParam("uuid") UUID uuid) {
    return messagesManager.delete(
            auth.getAccount().getUuid(),
            auth.getAuthenticatedDevice(),
            uuid,
            null)
        .thenAccept(maybeRemovedMessage -> maybeRemovedMessage.ifPresent(removedMessage -> {

          WebSocketConnection.recordMessageDeliveryDuration(removedMessage.serverTimestamp(),
              auth.getAuthenticatedDevice());

          if (removedMessage.sourceServiceId().isPresent()
              && removedMessage.envelopeType() != Type.SERVER_DELIVERY_RECEIPT) {
            if (removedMessage.sourceServiceId().get() instanceof AciServiceIdentifier aciServiceIdentifier) {
              try {
                receiptSender.sendReceipt(removedMessage.destinationServiceId(), auth.getAuthenticatedDevice().getId(),
                    aciServiceIdentifier, removedMessage.clientTimestamp());
              } catch (Exception e) {
                logger.warn("Failed to send delivery receipt", e);
              }
            } else {
              // If source service ID is present and the envelope type is not a server delivery receipt, then
              // the source service ID *should always* be an ACI -- PNIs are receive-only, so they can only be the
              // "source" via server delivery receipts
              logger.warn("Source service ID unexpectedly a PNI service ID");
            }
          }
        }))
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  @Timed
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/report/{source}/{messageGuid}")
  public Response reportSpamMessage(
      @ReadOnly @Auth AuthenticatedDevice auth,
      @PathParam("source") String source,
      @PathParam("messageGuid") UUID messageGuid,
      @Nullable SpamReport spamReport,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent
  ) {

    final Optional<String> sourceNumber;
    final Optional<UUID> sourceAci;
    final Optional<UUID> sourcePni;
    if (source.startsWith("+")) {
      sourceNumber = Optional.of(source);
      final Optional<Account> maybeAccount = accountsManager.getByE164(source);
      if (maybeAccount.isPresent()) {
        sourceAci = maybeAccount.map(Account::getUuid);
        sourcePni = maybeAccount.map(Account::getPhoneNumberIdentifier);
      } else {
        sourceAci = accountsManager.findRecentlyDeletedAccountIdentifier(source);
        sourcePni = Optional.ofNullable(accountsManager.getPhoneNumberIdentifier(source));
      }
    } else {
      sourceAci = Optional.of(UUID.fromString(source));

      final Optional<Account> sourceAccount = accountsManager.getByAccountIdentifier(sourceAci.get());

      if (sourceAccount.isEmpty()) {
        logger.warn("Could not find source: {}", sourceAci.get());
        sourceNumber = accountsManager.findRecentlyDeletedE164(sourceAci.get());
        sourcePni = sourceNumber.map(accountsManager::getPhoneNumberIdentifier);
      } else {
        sourceNumber = sourceAccount.map(Account::getNumber);
        sourcePni = sourceAccount.map(Account::getPhoneNumberIdentifier);
      }
    }

    UUID spamReporterUuid = auth.getAccount().getUuid();

    // spam report token is optional, but if provided ensure it is non-empty.
    final Optional<byte[]> maybeSpamReportToken =
        Optional.ofNullable(spamReport)
            .flatMap(r -> Optional.ofNullable(r.token()))
            .filter(t -> t.length > 0);

    reportMessageManager.report(sourceNumber, sourceAci, sourcePni, messageGuid, spamReporterUuid, maybeSpamReportToken, userAgent);

    return Response.status(Status.ACCEPTED)
        .build();
  }

  private void sendIndividualMessage(
      Optional<AuthenticatedDevice> source,
      Account destinationAccount,
      Device destinationDevice,
      ServiceIdentifier destinationIdentifier,
      long timestamp,
      boolean online,
      boolean story,
      boolean urgent,
      IncomingMessage incomingMessage,
      String userAgentString,
      Optional<byte[]> spamReportToken) {

    final Envelope envelope;

    try {
      final Account sourceAccount = source.map(AuthenticatedDevice::getAccount).orElse(null);
      final Byte sourceDeviceId = source.map(account -> account.getAuthenticatedDevice().getId()).orElse(null);
      envelope = incomingMessage.toEnvelope(
          destinationIdentifier,
          sourceAccount,
          sourceDeviceId,
          timestamp == 0 ? System.currentTimeMillis() : timestamp,
          story,
          urgent,
          spamReportToken.orElse(null));
    } catch (final IllegalArgumentException e) {
      logger.warn("Received bad envelope type {} from {}", incomingMessage.type(), userAgentString);
      throw new BadRequestException(e);
    }

    messageSender.sendMessage(destinationAccount, destinationDevice, envelope, online);
  }

  private void sendCommonPayloadMessage(Account destinationAccount,
      Device destinationDevice,
      ServiceIdentifier serviceIdentifier,
      long timestamp,
      boolean online,
      boolean story,
      boolean urgent,
      byte[] payload,
      byte[] sharedMrmKey) {

    final Envelope.Builder messageBuilder = Envelope.newBuilder();
    final long serverTimestamp = System.currentTimeMillis();

    messageBuilder
        .setType(Type.UNIDENTIFIED_SENDER)
        .setClientTimestamp(timestamp == 0 ? serverTimestamp : timestamp)
        .setServerTimestamp(serverTimestamp)
        .setStory(story)
        .setUrgent(urgent)
        .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
        .setSharedMrmKey(ByteString.copyFrom(sharedMrmKey));

    messageSender.sendMessage(destinationAccount, destinationDevice, messageBuilder.build(), online);
  }

  private void checkMessageRateLimit(AuthenticatedDevice source, Account destination, String userAgent)
      throws RateLimitExceededException {
    final String senderCountryCode = Util.getCountryCode(source.getAccount().getNumber());

    try {
      rateLimiters.getMessagesLimiter().validate(source.getAccount().getUuid(), destination.getUuid());
    } catch (final RateLimitExceededException e) {
      Metrics.counter(RATE_LIMITED_MESSAGE_COUNTER_NAME,
          Tags.of(
              UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of(SENDER_COUNTRY_TAG_NAME, senderCountryCode),
              Tag.of(RATE_LIMIT_REASON_TAG_NAME, "singleDestinationRate"))).increment();

      throw e;
    }
  }

  private void validateContentLength(final int contentLength, final boolean multiRecipientMessage, final String userAgent) {
    final boolean oversize = contentLength > MAX_MESSAGE_SIZE;

    DistributionSummary.builder(CONTENT_SIZE_DISTRIBUTION_NAME)
        .tags(Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of("oversize", String.valueOf(oversize)),
            Tag.of("multiRecipientMessage", String.valueOf(multiRecipientMessage))))
        .publishPercentileHistogram(true)
        .register(Metrics.globalRegistry)
        .record(contentLength);

    if (oversize) {
      Metrics.counter(REJECT_OVERSIZE_MESSAGE_COUNTER, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent)))
          .increment();
      throw new WebApplicationException(Status.REQUEST_ENTITY_TOO_LARGE);
    }
  }

  private void validateEnvelopeType(final int type, final String userAgent) {
    if (type == Type.SERVER_DELIVERY_RECEIPT_VALUE) {
      Metrics.counter(REJECT_INVALID_ENVELOPE_TYPE,
              Tags.of(UserAgentTagUtil.getPlatformTag(userAgent), Tag.of(ENVELOPE_TYPE_TAG_NAME, String.valueOf(type))))
          .increment();
      throw new BadRequestException("reserved envelope type");
    }
  }

  public static Optional<byte[]> getMessageContent(IncomingMessage message) {
    if (StringUtils.isEmpty(message.content())) return Optional.empty();

    try {
      return Optional.of(Base64.getDecoder().decode(message.content()));
    } catch (IllegalArgumentException e) {
      logger.debug("Bad B64", e);
      return Optional.empty();
    }
  }
}
