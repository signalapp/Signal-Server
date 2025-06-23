/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
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
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.ServiceId;
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
import org.whispersystems.textsecuregcm.entities.AccountMismatchedDevices;
import org.whispersystems.textsecuregcm.entities.AccountStaleDevices;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope.Type;
import org.whispersystems.textsecuregcm.entities.MismatchedDevicesResponse;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;
import org.whispersystems.textsecuregcm.entities.SendMultiRecipientMessageResponse;
import org.whispersystems.textsecuregcm.entities.SpamReport;
import org.whispersystems.textsecuregcm.entities.StaleDevicesResponse;
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
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.push.MessageUtil;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.PushNotificationScheduler;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.spam.MessageType;
import org.whispersystems.textsecuregcm.spam.SpamCheckResult;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;
import org.whispersystems.websocket.WebsocketHeaders;
import reactor.core.scheduler.Scheduler;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/messages")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Messages")
public class MessageController {

  private static final Logger logger = LoggerFactory.getLogger(MessageController.class);

  private final RateLimiters rateLimiters;
  private final CardinalityEstimator messageByteLimitEstimator;
  private final MessageSender messageSender;
  private final ReceiptSender receiptSender;
  private final AccountsManager accountsManager;
  private final MessagesManager messagesManager;
  private final PhoneNumberIdentifiers phoneNumberIdentifiers;
  private final PushNotificationManager pushNotificationManager;
  private final PushNotificationScheduler pushNotificationScheduler;
  private final ReportMessageManager reportMessageManager;
  private final Scheduler messageDeliveryScheduler;
  private final ClientReleaseManager clientReleaseManager;
  private final ServerSecretParams serverSecretParams;
  private final SpamChecker spamChecker;
  private final MessageMetrics messageMetrics;
  private final MessageDeliveryLoopMonitor messageDeliveryLoopMonitor;
  private final Clock clock;

  private static final CompletableFuture<?>[] EMPTY_FUTURE_ARRAY = new CompletableFuture<?>[0];

  private static final String OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME = name(MessageController.class, "outgoingMessageListSizeBytes");

  private static final Timer INDIVIDUAL_MESSAGE_LATENCY_TIMER;
  private static final Timer MULTI_RECIPIENT_MESSAGE_LATENCY_TIMER;

  static {
    final String timerName = MetricsUtil.name(MessageController.class, "sendMessageLatency");
    final String multiRecipientTagName = "multiRecipient";

    INDIVIDUAL_MESSAGE_LATENCY_TIMER = Timer.builder(timerName)
        .tags(multiRecipientTagName, "false")
        .publishPercentileHistogram(true)
        .register(Metrics.globalRegistry);

    MULTI_RECIPIENT_MESSAGE_LATENCY_TIMER = Timer.builder(timerName)
        .tags(multiRecipientTagName, "true")
        .publishPercentileHistogram(true)
        .register(Metrics.globalRegistry);
  }

  // The Signal desktop client (really, JavaScript in general) can handle message timestamps at most 100,000,000 days
  // past the epoch; please see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date#the_epoch_timestamps_and_invalid_date
  // for additional details.
  public static final long MAX_TIMESTAMP = 86_400_000L * 100_000_000L;

  private static final Duration NOTIFY_FOR_REMAINING_MESSAGES_DELAY = Duration.ofMinutes(1);

  private static final SendMultiRecipientMessageResponse SEND_STORY_RESPONSE =
      new SendMultiRecipientMessageResponse(Collections.emptyList());

  public MessageController(
      RateLimiters rateLimiters,
      CardinalityEstimator messageByteLimitEstimator,
      MessageSender messageSender,
      ReceiptSender receiptSender,
      AccountsManager accountsManager,
      MessagesManager messagesManager,
      PhoneNumberIdentifiers phoneNumberIdentifiers,
      PushNotificationManager pushNotificationManager,
      PushNotificationScheduler pushNotificationScheduler,
      ReportMessageManager reportMessageManager,
      Scheduler messageDeliveryScheduler,
      final ClientReleaseManager clientReleaseManager,
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
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.pushNotificationManager = pushNotificationManager;
    this.pushNotificationScheduler = pushNotificationScheduler;
    this.reportMessageManager = reportMessageManager;
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.clientReleaseManager = clientReleaseManager;
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
      content = @Content(schema = @Schema(implementation = MismatchedDevicesResponse.class)))
  @ApiResponse(
      responseCode = "410", description = "Mismatched registration ids supplied for some recipient devices",
      content = @Content(schema = @Schema(implementation = StaleDevicesResponse.class)))
  @ApiResponse(
      responseCode="428",
      description="The sender should complete a challenge before proceeding")
  public Response sendMessage(@Auth final Optional<AuthenticatedDevice> source,
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

    if (groupSendToken != null) {
      if (source.isPresent() || accessKey.isPresent()) {
        throw new BadRequestException("Group send endorsement tokens should not be combined with other authentication");
      } else if (isStory) {
        throw new BadRequestException("Group send endorsement tokens should not be sent for story messages");
      }
    }

    final Sample sample = Timer.start();
    final boolean needsSync;

    try {
      if (isStory) {
        needsSync = false;
        sendStoryMessage(destinationIdentifier, messages, context);
      } else if (source.isPresent()) {
        final AuthenticatedDevice authenticatedDevice = source.get();
        final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
            .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

        if (account.isIdentifiedBy(destinationIdentifier)) {
          needsSync = false;
          sendSyncMessage(source.get(), account, destinationIdentifier, messages, context);
        } else {
          needsSync = account.getDevices().size() > 1;
          sendIdentifiedSenderIndividualMessage(authenticatedDevice, destinationIdentifier, messages, context);
        }
      } else {
        needsSync = false;
        sendSealedSenderMessage(destinationIdentifier, messages, accessKey, groupSendToken != null ? groupSendToken.token() : null, context);
      }
    } finally {
      sample.stop(INDIVIDUAL_MESSAGE_LATENCY_TIMER);
    }

    return Response.ok(new SendMessageResponse(needsSync)).build();
  }

  private void sendIdentifiedSenderIndividualMessage(final AuthenticatedDevice source,
      final ServiceIdentifier destinationIdentifier,
      final IncomingMessageList messages,
      final ContainerRequestContext context)
      throws RateLimitExceededException {

    final Account destination =
        accountsManager.getByServiceIdentifier(destinationIdentifier).orElseThrow(NotFoundException::new);

    rateLimiters.getMessagesLimiter().validate(source.accountIdentifier(), destination.getUuid());

    sendIndividualMessage(destination,
        destinationIdentifier,
        source,
        messages,
        false,
        MessageType.INDIVIDUAL_IDENTIFIED_SENDER,
        context);
  }

  private void sendSyncMessage(final AuthenticatedDevice source,
      final Account sourceAccount,
      final ServiceIdentifier destinationIdentifier,
      final IncomingMessageList messages,
      final ContainerRequestContext context)
      throws RateLimitExceededException {

    if (destinationIdentifier.identityType() == IdentityType.PNI) {
      throw new WebApplicationException(Status.FORBIDDEN);
    }

    sendIndividualMessage(sourceAccount,
        destinationIdentifier,
        source,
        messages,
        false,
        MessageType.SYNC,
        context);
  }

  private void sendSealedSenderMessage(final ServiceIdentifier destinationIdentifier,
      final IncomingMessageList messages,
      final Optional<Anonymous> accessKey,
      @Nullable final GroupSendFullToken groupSendToken,
      final ContainerRequestContext context)
      throws RateLimitExceededException {

    if (accessKey.isEmpty() && groupSendToken == null) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }

    final Optional<Account> maybeDestination = accountsManager.getByServiceIdentifier(destinationIdentifier);

    if (groupSendToken != null) {
      checkGroupSendToken(List.of(destinationIdentifier.toLibsignal()), groupSendToken);
    } else {
      OptionalAccess.verify(Optional.empty(), accessKey, maybeDestination, destinationIdentifier);
    }

    final Account destination = maybeDestination.orElseThrow(NotFoundException::new);

    sendIndividualMessage(destination,
        destinationIdentifier,
        null,
        messages,
        false,
        MessageType.INDIVIDUAL_SEALED_SENDER,
        context);
  }

  private void sendStoryMessage(final ServiceIdentifier destinationIdentifier,
      final IncomingMessageList messages,
      final ContainerRequestContext context)
      throws RateLimitExceededException {

    // We return 200 when stories are sent to a non-existent account. Since story sends bypass OptionalAccess.verify
    // authentication is handled by the receiving client, we leak information about whether a destination UUID exists if
    // we return any other code (e.g. 404) from these requests.
    final Account destination = accountsManager.getByServiceIdentifier(destinationIdentifier).orElseThrow(() ->
        new WebApplicationException(Response.ok(new SendMessageResponse(false)).build()));

    rateLimiters.getStoriesLimiter().validate(destination.getUuid());

    sendIndividualMessage(destination,
        destinationIdentifier,
        null,
        messages,
        true,
        MessageType.INDIVIDUAL_STORY,
        context);
  }

  private void sendIndividualMessage(final Account destination,
      final ServiceIdentifier destinationIdentifier,
      @Nullable final AuthenticatedDevice sender,
      final IncomingMessageList messages,
      final boolean isStory,
      final MessageType messageType,
      final ContainerRequestContext context) throws RateLimitExceededException {

    final SpamCheckResult<Response> spamCheckResult =
        spamChecker.checkForIndividualRecipientSpamHttp(messageType,
            context,
            Optional.ofNullable(sender),
            Optional.of(destination),
            destinationIdentifier);

    spamCheckResult.response().ifPresent(response -> {
      throw new WebApplicationException(response);
    });

    final String userAgent = context.getHeaderString(HttpHeaders.USER_AGENT);

    try {
      final int totalContentLength =
          messages.messages().stream().mapToInt(message -> message.content().length).sum();

      rateLimiters.getInboundMessageBytes().validate(destinationIdentifier.uuid(), totalContentLength);
    } catch (final RateLimitExceededException e) {
      messageByteLimitEstimator.add(destinationIdentifier.uuid().toString());
      throw e;
    }

    final Map<Byte, Envelope> messagesByDeviceId = messages.messages().stream()
        .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, message -> {
          try {
            return message.toEnvelope(
                destinationIdentifier,
                sender != null ? new AciServiceIdentifier(sender.accountIdentifier()) : null,
                sender != null ? sender.deviceId() : null,
                messages.timestamp() == 0 ? System.currentTimeMillis() : messages.timestamp(),
                isStory,
                messages.online(),
                messages.urgent(),
                spamCheckResult.token().orElse(null));
          } catch (final IllegalArgumentException e) {
            logger.warn("Received bad envelope type {} from {}", message.type(), userAgent);
            throw new BadRequestException(e);
          }
        }));

    final Map<Byte, Integer> registrationIdsByDeviceId = messages.messages().stream()
        .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, IncomingMessage::destinationRegistrationId));

    final Optional<Byte> syncMessageSenderDeviceId = messageType == MessageType.SYNC
        ? Optional.ofNullable(sender).map(AuthenticatedDevice::deviceId)
        : Optional.empty();

    try {
      messageSender.sendMessages(destination,
          destinationIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          syncMessageSenderDeviceId,
          userAgent);
    } catch (final MismatchedDevicesException e) {
      if (!e.getMismatchedDevices().staleDeviceIds().isEmpty()) {
        throw new WebApplicationException(Response.status(410)
            .type(MediaType.APPLICATION_JSON)
            .entity(new StaleDevicesResponse(e.getMismatchedDevices().staleDeviceIds()))
            .build());
      } else {
        throw new WebApplicationException(Response.status(409)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(new MismatchedDevicesResponse(e.getMismatchedDevices().missingDeviceIds(),
                e.getMismatchedDevices().extraDeviceIds()))
            .build());
      }
    } catch (final MessageTooLargeException e) {
      throw new WebApplicationException(Status.REQUEST_ENTITY_TOO_LARGE);
    }
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
  @ApiResponse(
      responseCode="200",
      description="Message was successfully sent",
      content = @Content(schema = @Schema(implementation = SendMultiRecipientMessageResponse.class)))
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

      @Context ContainerRequestContext context) {

    if (timestamp < 0 || timestamp > MAX_TIMESTAMP) {
      throw new BadRequestException("Illegal timestamp");
    }

    if (multiRecipientMessage.getRecipients().isEmpty()) {
      throw new BadRequestException("Recipient list is empty");
    }

    final Timer.Sample sample = Timer.start();

    try {
      final SendMultiRecipientMessageResponse sendMultiRecipientMessageResponse;

      if (isStory) {
        if (groupSendToken != null) {
          // Stories require no authentication. We fail requests that provide a groupSendToken, but for historical
          // reasons we allow requests to set a combined access key, even though we ignore it
          throw new BadRequestException("Group send token not allowed when sending stories");
        }

        sendMultiRecipientMessageResponse =
            sendMultiRecipientStoryMessage(multiRecipientMessage, timestamp, online, isUrgent, context);
      } else {
        sendMultiRecipientMessageResponse =
            sendMultiRecipientMessage(multiRecipientMessage, timestamp, online, isUrgent, groupSendToken, accessKeys,
                context);
      }

      return Response.ok(sendMultiRecipientMessageResponse).build();
    } finally {
      sample.stop(MULTI_RECIPIENT_MESSAGE_LATENCY_TIMER);
    }
  }

  private SendMultiRecipientMessageResponse sendMultiRecipientMessage(final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final long timestamp,
      final boolean ephemeral,
      final boolean urgent,
      @Nullable final GroupSendTokenHeader groupSendTokenHeader,
      @Nullable final CombinedUnidentifiedSenderAccessKeys combinedUnidentifiedSenderAccessKeys,
      final ContainerRequestContext context) {

    // Perform fast, inexpensive checks before attempting to resolve recipients
    if (MessageUtil.hasDuplicateDevices(multiRecipientMessage)) {
      throw new BadRequestException("Multi-recipient message contains duplicate recipient");
    }

    if (groupSendTokenHeader == null && combinedUnidentifiedSenderAccessKeys == null) {
      throw new NotAuthorizedException("A group send endorsement token or unidentified access key is required for non-story messages");
    }

    if (groupSendTokenHeader != null && combinedUnidentifiedSenderAccessKeys != null) {
      throw new BadRequestException("Only one of group send endorsement token and unidentified access key may be provided");
    }

    if (groupSendTokenHeader != null) {
      // Group send endorsements are checked before we even attempt to resolve any accounts, since
      // the lists of service IDs in the envelope are all that we need to check against
      checkGroupSendToken(multiRecipientMessage.getRecipients().keySet(), groupSendTokenHeader);
    }

    // At this point, the caller has at least superficially provided the information needed to send a multi-recipient
    // message. Attempt to resolve the destination service identifiers to Signal accounts.
    final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients =
        MessageUtil.resolveRecipients(accountsManager, multiRecipientMessage);

    final List<ServiceIdentifier> unresolvedRecipientServiceIdentifiers =
        MessageUtil.getUnresolvedRecipients(multiRecipientMessage, resolvedRecipients);

    if (groupSendTokenHeader == null && !unresolvedRecipientServiceIdentifiers.isEmpty()) {
      throw new NotFoundException();
    }

    // Access keys are checked against the UAK in the resolved accounts, so we have to check after resolving accounts above.
    // Group send endorsements are checked earlier; for stories, we don't check permissions at all because only clients check them
    if (groupSendTokenHeader == null) {
      checkAccessKeys(combinedUnidentifiedSenderAccessKeys, multiRecipientMessage, resolvedRecipients);
    }

    sendMultiRecipientMessage(multiRecipientMessage,
        resolvedRecipients,
        timestamp,
        false,
        ephemeral,
        urgent,
        context);

    return new SendMultiRecipientMessageResponse(unresolvedRecipientServiceIdentifiers);
  }

  @SuppressWarnings("SameReturnValue")
  private SendMultiRecipientMessageResponse sendMultiRecipientStoryMessage(final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final long timestamp,
      final boolean ephemeral,
      final boolean urgent,
      final ContainerRequestContext context) {

    // Perform fast, inexpensive checks before attempting to resolve recipients
    if (MessageUtil.hasDuplicateDevices(multiRecipientMessage)) {
      throw new BadRequestException("Multi-recipient message contains duplicate recipient");
    }

    // At this point, the caller has at least superficially provided the information needed to send a multi-recipient
    // message. Attempt to resolve the destination service identifiers to Signal accounts.
    final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients =
        MessageUtil.resolveRecipients(accountsManager, multiRecipientMessage);

    // We might filter out all the recipients of a story (if none exist).
    // In this case there is no error so we should just return 200 now.
    if (resolvedRecipients.isEmpty()) {
      return SEND_STORY_RESPONSE;
    }

    CompletableFuture.allOf(resolvedRecipients.values()
            .stream()
            .map(account -> account.getIdentifier(IdentityType.ACI))
            .map(accountIdentifier ->
                rateLimiters.getStoriesLimiter().validateAsync(accountIdentifier).toCompletableFuture())
            .toList()
            .toArray(EMPTY_FUTURE_ARRAY))
        .join();

    sendMultiRecipientMessage(multiRecipientMessage,
        resolvedRecipients,
        timestamp,
        true,
        ephemeral,
        urgent,
        context);

    return SEND_STORY_RESPONSE;
  }

  private void sendMultiRecipientMessage(final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients,
      final long timestamp,
      final boolean isStory,
      final boolean ephemeral,
      final boolean urgent,
      final ContainerRequestContext context) {

    final MessageType messageType =
        isStory ? MessageType.MULTI_RECIPIENT_STORY : MessageType.MULTI_RECIPIENT_SEALED_SENDER;

    spamChecker.checkForMultiRecipientSpamHttp(messageType, context).response().ifPresent(response -> {
      throw new WebApplicationException(response);
    });

    try {
      if (!resolvedRecipients.isEmpty()) {
        messageSender.sendMultiRecipientMessage(multiRecipientMessage,
            resolvedRecipients,
            timestamp, isStory,
            ephemeral,
            urgent,
            context.getHeaderString(HttpHeaders.USER_AGENT)).get();
      }
    } catch (final InterruptedException e) {
      logger.error("interrupted while delivering multi-recipient messages", e);
      throw new InternalServerErrorException("interrupted during delivery");
    } catch (final CancellationException e) {
      logger.error("cancelled while delivering multi-recipient messages", e);
      throw new InternalServerErrorException("delivery cancelled");
    } catch (final ExecutionException e) {
      logger.error("partial failure while delivering multi-recipient messages", e.getCause());
      throw new InternalServerErrorException("failure during delivery");
    } catch (final MessageTooLargeException e) {
      throw new WebApplicationException(Status.REQUEST_ENTITY_TOO_LARGE);
    } catch (final MultiRecipientMismatchedDevicesException e) {
      final List<AccountMismatchedDevices> accountMismatchedDevices =
          e.getMismatchedDevicesByServiceIdentifier().entrySet().stream()
              .filter(entry -> !entry.getValue().missingDeviceIds().isEmpty() || !entry.getValue().extraDeviceIds().isEmpty())
              .map(entry -> new AccountMismatchedDevices(entry.getKey(),
                  new MismatchedDevicesResponse(entry.getValue().missingDeviceIds(), entry.getValue().extraDeviceIds())))
              .toList();

      if (!accountMismatchedDevices.isEmpty()) {
        throw new WebApplicationException(Response
            .status(409)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(accountMismatchedDevices)
            .build());
      }

      final List<AccountStaleDevices> accountStaleDevices =
          e.getMismatchedDevicesByServiceIdentifier().entrySet().stream()
              .filter(entry -> !entry.getValue().staleDeviceIds().isEmpty())
              .map(entry -> new AccountStaleDevices(entry.getKey(),
                  new StaleDevicesResponse(entry.getValue().staleDeviceIds())))
              .toList();

      throw new WebApplicationException(Response
          .status(410)
          .type(MediaType.APPLICATION_JSON)
          .entity(accountStaleDevices)
          .build());
    }
  }

  private void checkGroupSendToken(final Collection<ServiceId> recipients, final GroupSendTokenHeader groupSendToken) {
    checkGroupSendToken(recipients, groupSendToken.token());
  }

  private void checkGroupSendToken(final Collection<ServiceId> recipients, final GroupSendFullToken groupSendFullToken) {
    try {
      groupSendFullToken.verify(recipients,
          clock.instant(),
          GroupSendDerivedKeyPair.forExpiration(groupSendFullToken.getExpiration(), serverSecretParams));
    } catch (final VerificationFailedException e) {
      throw new NotAuthorizedException(e);
    }
  }

  private void checkAccessKeys(
      final @NotNull CombinedUnidentifiedSenderAccessKeys accessKeys,
      final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients) {

    if (multiRecipientMessage.getRecipients().keySet().stream()
        .anyMatch(serviceId -> serviceId instanceof ServiceId.Pni)) {

      throw new WebApplicationException("Multi-recipient messages must be addressed to ACI service IDs",
          Status.UNAUTHORIZED);
    }

    try {
      if (!UnidentifiedAccessUtil.checkUnidentifiedAccess(resolvedRecipients.values(), accessKeys.getAccessKeys())) {
        throw new WebApplicationException(Status.UNAUTHORIZED);
      }
    } catch (final IllegalArgumentException ignored) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public CompletableFuture<OutgoingMessageEntityList> getPendingMessages(@Auth AuthenticatedDevice auth,
      @HeaderParam(WebsocketHeaders.X_SIGNAL_RECEIVE_STORIES) String receiveStoriesHeader,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent) {

    return accountsManager.getByAccountIdentifierAsync(auth.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount.orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));
          final Device device = account.getDevice(auth.deviceId())
              .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

          final boolean shouldReceiveStories = WebsocketHeaders.parseReceiveStoriesHeader(receiveStoriesHeader);

          pushNotificationManager.handleMessagesRetrieved(account, device, userAgent);

          return messagesManager.getMessagesForDevice(
                  auth.accountIdentifier(),
                  device,
                  false)
              .map(messagesAndHasMore -> {
                Stream<Envelope> envelopes = messagesAndHasMore.first().stream();
                if (!shouldReceiveStories) {
                  envelopes = envelopes.filter(e -> !e.getStory());
                }

          final OutgoingMessageEntityList messages = new OutgoingMessageEntityList(envelopes
              .map(OutgoingMessageEntity::fromEnvelope)
              .peek(outgoingMessageEntity -> {
                messageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageEntity);
                messageMetrics.measureOutgoingMessageLatency(outgoingMessageEntity.serverTimestamp(),
                    "rest",
                    auth.deviceId() == Device.PRIMARY_ID,
                    outgoingMessageEntity.urgent(),
                    // Messages fetched via this endpoint (as opposed to WebSocketConnection) are never ephemeral
                    // because, by definition, the client doesn't have a "live" connection via which to receive
                    // ephemeral messages.
                    false,
                    userAgent,
                    clientReleaseManager);
              })
              .collect(Collectors.toList()),
              messagesAndHasMore.second());

                Metrics.summary(OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent)))
                    .record(estimateMessageListSizeBytes(messages));

                if (!messages.messages().isEmpty()) {
                  messageDeliveryLoopMonitor.recordDeliveryAttempt(auth.accountIdentifier(),
                      auth.deviceId(),
                      messages.messages().getFirst().guid(),
                      userAgent,
                      "rest");
                }

                if (messagesAndHasMore.second()) {
                  pushNotificationScheduler.scheduleDelayedNotification(account, device, NOTIFY_FOR_REMAINING_MESSAGES_DELAY);
                }

                return messages;
              })
              .timeout(Duration.ofSeconds(5))
              .subscribeOn(messageDeliveryScheduler)
              .toFuture();
        });
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
  public CompletableFuture<Response> removePendingMessage(@Auth AuthenticatedDevice auth, @PathParam("uuid") UUID uuid) {
    final Account account = accountsManager.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final Device device = account.getDevice(auth.deviceId())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    return messagesManager.delete(
            auth.accountIdentifier(),
            device,
            uuid,
            null)
        .thenAccept(maybeRemovedMessage -> maybeRemovedMessage.ifPresent(removedMessage -> {

          WebSocketConnection.recordMessageDeliveryDuration(removedMessage.serverTimestamp(), device);

          if (removedMessage.sourceServiceId().isPresent()
              && removedMessage.envelopeType() != Type.SERVER_DELIVERY_RECEIPT) {
            if (removedMessage.sourceServiceId().get() instanceof AciServiceIdentifier aciServiceIdentifier) {
              try {
                receiptSender.sendReceipt(removedMessage.destinationServiceId(), auth.deviceId(),
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
      @Auth AuthenticatedDevice auth,
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
        sourcePni = Optional.ofNullable(phoneNumberIdentifiers.getPhoneNumberIdentifier(source).join());
        sourceAci = sourcePni.flatMap(accountsManager::findRecentlyDeletedAccountIdentifier);
      }
    } else {
      sourceAci = Optional.of(UUID.fromString(source));

      final Optional<Account> sourceAccount = accountsManager.getByAccountIdentifier(sourceAci.get());

      if (sourceAccount.isEmpty()) {
        logger.warn("Could not find source: {}", sourceAci.get());
        sourcePni = accountsManager.findRecentlyDeletedPhoneNumberIdentifier(sourceAci.get());
        sourceNumber = sourcePni.flatMap(pni ->
            Util.getCanonicalNumber(phoneNumberIdentifiers.getPhoneNumber(pni).join()));
      } else {
        sourceNumber = sourceAccount.map(Account::getNumber);
        sourcePni = sourceAccount.map(Account::getPhoneNumberIdentifier);
      }
    }

    UUID spamReporterUuid = auth.accountIdentifier();

    // spam report token is optional, but if provided ensure it is non-empty.
    final Optional<byte[]> maybeSpamReportToken =
        Optional.ofNullable(spamReport)
            .flatMap(r -> Optional.ofNullable(r.token()))
            .filter(t -> t.length > 0);

    reportMessageManager.report(sourceNumber, sourceAci, sourcePni, messageGuid, spamReporterUuid, maybeSpamReportToken, userAgent);

    return Response.status(Status.ACCEPTED)
        .build();
  }
}
