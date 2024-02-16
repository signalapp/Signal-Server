/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.Auth;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.Counter;
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

import java.security.MessageDigest;
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
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ManagedAsync;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage.Recipient;
import org.signal.libsignal.protocol.util.Pair;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CombinedUnidentifiedSenderAccessKeys;
import org.whispersystems.textsecuregcm.auth.GroupSendCredentialHeader;
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
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.spam.ReportSpamTokenProvider;
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
  private final ReportMessageManager reportMessageManager;
  private final ExecutorService multiRecipientMessageExecutor;
  private final Scheduler messageDeliveryScheduler;
  private final ReportSpamTokenProvider reportSpamTokenProvider;
  private final ClientReleaseManager clientReleaseManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final ServerSecretParams serverSecretParams;
  private final SpamChecker spamChecker;

  private static final int MAX_FETCH_ACCOUNT_CONCURRENCY = 8;

  private static final CompletableFuture<?>[] EMPTY_FUTURE_ARRAY = new CompletableFuture<?>[0];

  private static final String REJECT_OVERSIZE_MESSAGE_COUNTER = name(MessageController.class, "rejectOversizeMessage");
  private static final String SENT_MESSAGE_COUNTER_NAME = name(MessageController.class, "sentMessages");
  private static final String CONTENT_SIZE_DISTRIBUTION_NAME = name(MessageController.class, "messageContentSize");
  private static final String OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME = name(MessageController.class, "outgoingMessageListSizeBytes");
  private static final String RATE_LIMITED_MESSAGE_COUNTER_NAME = name(MessageController.class, "rateLimitedMessage");

  private static final String REJECT_INVALID_ENVELOPE_TYPE = name(MessageController.class, "rejectInvalidEnvelopeType");
  private static final String UNEXPECTED_MISSING_USER_COUNTER_NAME = name(MessageController.class, "unexpectedMissingDestinationForMultiRecipientMessage");
  private static final Timer SEND_MESSAGE_LATENCY_TIMER =
      Timer.builder(MetricsUtil.name(MessageController.class, "sendMessageLatency"))
          .publishPercentileHistogram(true)
          .register(Metrics.globalRegistry);

  private static final String EPHEMERAL_TAG_NAME = "ephemeral";
  private static final String SENDER_TYPE_TAG_NAME = "senderType";
  private static final String SENDER_COUNTRY_TAG_NAME = "senderCountry";
  private static final String RATE_LIMIT_REASON_TAG_NAME = "rateLimitReason";
  private static final String ENVELOPE_TYPE_TAG_NAME = "envelopeType";
  private static final String IDENTITY_TYPE_TAG_NAME = "identityType";

  private static final String SENDER_TYPE_IDENTIFIED = "identified";
  private static final String SENDER_TYPE_UNIDENTIFIED = "unidentified";
  private static final String SENDER_TYPE_SELF = "self";

  @VisibleForTesting
  static final long MAX_MESSAGE_SIZE = DataSize.kibibytes(256).toBytes();

  public MessageController(
      RateLimiters rateLimiters,
      CardinalityEstimator messageByteLimitEstimator,
      MessageSender messageSender,
      ReceiptSender receiptSender,
      AccountsManager accountsManager,
      MessagesManager messagesManager,
      PushNotificationManager pushNotificationManager,
      ReportMessageManager reportMessageManager,
      @Nonnull ExecutorService multiRecipientMessageExecutor,
      Scheduler messageDeliveryScheduler,
      @Nonnull ReportSpamTokenProvider reportSpamTokenProvider,
      final ClientReleaseManager clientReleaseManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ServerSecretParams serverSecretParams,
      final SpamChecker spamChecker) {
    this.rateLimiters = rateLimiters;
    this.messageByteLimitEstimator = messageByteLimitEstimator;
    this.messageSender = messageSender;
    this.receiptSender = receiptSender;
    this.accountsManager = accountsManager;
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
    this.reportMessageManager = reportMessageManager;
    this.multiRecipientMessageExecutor = Objects.requireNonNull(multiRecipientMessageExecutor);
    this.messageDeliveryScheduler = messageDeliveryScheduler;
    this.reportSpamTokenProvider = reportSpamTokenProvider;
    this.clientReleaseManager = clientReleaseManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.serverSecretParams = serverSecretParams;
    this.spamChecker = spamChecker;
  }

  @Timed
  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ManagedAsync
  public Response sendMessage(@ReadOnly @Auth Optional<AuthenticatedAccount> source,
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) Optional<Anonymous> accessKey,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,
      @PathParam("destination") ServiceIdentifier destinationIdentifier,
      @QueryParam("story") boolean isStory,
      @NotNull @Valid IncomingMessageList messages,
      @Context ContainerRequestContext context) throws RateLimitExceededException {

    final Sample sample = Timer.start();
    try {
      if (source.isEmpty() && accessKey.isEmpty() && !isStory) {
        throw new WebApplicationException(Response.Status.UNAUTHORIZED);
      }

      final String senderType;
      if (source.isPresent()) {
        if (source.get().getAccount().isIdentifiedBy(destinationIdentifier)) {
          senderType = SENDER_TYPE_SELF;
        } else {
          senderType = SENDER_TYPE_IDENTIFIED;
        }
      } else {
        senderType = SENDER_TYPE_UNIDENTIFIED;
      }

      boolean isSyncMessage = source.isPresent() && source.get().getAccount().isIdentifiedBy(destinationIdentifier);

      if (isSyncMessage && destinationIdentifier.identityType() == IdentityType.PNI) {
        throw new WebApplicationException(Status.FORBIDDEN);
      }

      Optional<Account> destination;
      if (!isSyncMessage) {
        destination = accountsManager.getByServiceIdentifier(destinationIdentifier);
      } else {
        destination = source.map(AuthenticatedAccount::getAccount);
      }

      final Optional<Response> spamCheck = spamChecker.checkForSpam(
          context, source.map(AuthenticatedAccount::getAccount), destination);
      if (spamCheck.isPresent()) {
        return spamCheck.get();
      }

      final Optional<byte[]> spamReportToken = switch (senderType) {
        case SENDER_TYPE_IDENTIFIED ->
        reportSpamTokenProvider.makeReportSpamToken(context, source.get().getAccount(), destination);
        default -> Optional.empty();
      };

      int totalContentLength = 0;

      for (final IncomingMessage message : messages.messages()) {
        int contentLength = 0;

        if (StringUtils.isNotEmpty(message.content())) {
          contentLength += message.content().length();
        }

        validateContentLength(contentLength, userAgent);
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
        // Stories will be checked by the client; we bypass access checks here for stories.
        if (!isStory) {
          OptionalAccess.verify(source.map(AuthenticatedAccount::getAccount), accessKey, destination);
        }

        boolean needsSync = !isSyncMessage && source.isPresent() && source.get().getAccount().hasEnabledLinkedDevice();

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

        final List<Tag> tags = List.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(messages.online())),
            Tag.of(SENDER_TYPE_TAG_NAME, senderType),
            Tag.of(IDENTITY_TYPE_TAG_NAME, destinationIdentifier.identityType().name()));

        for (IncomingMessage incomingMessage : messages.messages()) {
          Optional<Device> destinationDevice = destination.get().getDevice(incomingMessage.destinationDeviceId());

          if (destinationDevice.isPresent()) {
            Metrics.counter(SENT_MESSAGE_COUNTER_NAME, tags).increment();
            sendIndividualMessage(
                source,
                destination.get(),
                destinationDevice.get(),
                destinationIdentifier,
                messages.timestamp(),
                messages.online(),
                isStory,
                messages.urgent(),
                incomingMessage,
                userAgent,
                spamReportToken);
          }
        }

        return Response.ok(new SendMessageResponse(needsSync)).build();
      } catch (NoSuchUserException e) {
        throw new WebApplicationException(Response.status(404).build());
      } catch (MismatchedDevicesException e) {
        throw new WebApplicationException(Response.status(409)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(new MismatchedDevices(e.getMissingDevices(),
                    e.getExtraDevices()))
            .build());
      } catch (StaleDevicesException e) {
        throw new WebApplicationException(Response.status(410)
            .type(MediaType.APPLICATION_JSON)
            .entity(new StaleDevices(e.getStaleDevices()))
            .build());
      }
    } finally {
      sample.stop(SEND_MESSAGE_LATENCY_TIMER);
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
      description="The message is not a story and the unauthorized access key or group send credential is missing or incorrect")
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
      @Parameter(description="The bitwise xor of the unidentified access keys for every recipient of the message. Will be replaced with group send credentials")
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) @Nullable CombinedUnidentifiedSenderAccessKeys accessKeys,

      @Parameter(description="A group send credential covering all (included and excluded) recipients of the message. Must not be combined with `Unidentified-Access-Key` or set on a story message.")
      @HeaderParam(HeaderUtils.GROUP_SEND_CREDENTIAL)
      @Nullable GroupSendCredentialHeader groupSendCredential,

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

    final Optional<Response> spamCheck = spamChecker.checkForSpam(context, Optional.empty(), Optional.empty());
    if (spamCheck.isPresent()) {
      return spamCheck.get();
    }

    if (groupSendCredential == null && accessKeys == null && !isStory) {
      throw new NotAuthorizedException("A group send credential or unidentified access key is required for non-story messages");
    }
    if (groupSendCredential != null) {
      if (accessKeys != null) {
        throw new BadRequestException("Only one of group send credential and unidentified access key may be provided");
      } else if (isStory) {
        throw new BadRequestException("Stories should not provide a group send credential");
      }
    }

    if (groupSendCredential != null) {
      // Group send credentials are checked before we even attempt to resolve any accounts, since
      // the lists of service IDs in the envelope are all that we need to check against
      checkGroupSendCredential(
          multiRecipientMessage.getRecipients().keySet(), multiRecipientMessage.getExcludedRecipients(), groupSendCredential);
    }

    final Map<ServiceIdentifier, MultiRecipientDeliveryData> recipients = buildRecipientMap(multiRecipientMessage, isStory);

    // Access keys are checked against the UAK in the resolved accounts, so we have to check after resolving accounts above.
    // Group send credentials are checked earlier; for stories, we don't check permissions at all because only clients check them
    if (groupSendCredential == null && !isStory) {
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
            DestinationDeviceValidator.validateCompleteDeviceList(account, recipient.deviceIdToRegistrationId().keySet(), Collections.emptySet());

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

    List<ServiceIdentifier> uuids404 = Collections.synchronizedList(new ArrayList<>());

    try {
      CompletableFuture.allOf(
          recipients.values().stream()
              .flatMap(recipientData -> {
                final Counter sentMessageCounter = Metrics.counter(SENT_MESSAGE_COUNTER_NAME, Tags.of(
                    UserAgentTagUtil.getPlatformTag(userAgent),
                    Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(online)),
                    Tag.of(SENDER_TYPE_TAG_NAME, SENDER_TYPE_UNIDENTIFIED),
                    Tag.of(IDENTITY_TYPE_TAG_NAME, recipientData.serviceIdentifier().identityType().name())));

                  return recipientData.deviceIdToRegistrationId().keySet().stream().map(
                      deviceId ->CompletableFuture.runAsync(
                          () -> {
                            final Account destinationAccount = recipientData.account();
                            final byte[] payload = multiRecipientMessage.messageForRecipient(recipientData.recipient());

                            // we asserted this must exist in validateCompleteDeviceList
                            final Device destinationDevice = destinationAccount.getDevice(deviceId).orElseThrow();
                            try {
                              sentMessageCounter.increment();
                              sendCommonPayloadMessage(
                                  destinationAccount, destinationDevice, recipientData.serviceIdentifier(), timestamp, online,
                                  isStory, isUrgent, payload);
                            } catch (NoSuchUserException e) {
                              // this should never happen, because we already asserted the device is present and enabled
                              Metrics.counter(
                                  UNEXPECTED_MISSING_USER_COUNTER_NAME,
                                  Tags.of("isPrimary", String.valueOf(destinationDevice.isPrimary()))).increment();
                              uuids404.add(recipientData.serviceIdentifier());
                            }
                          },
                          multiRecipientMessageExecutor));
              })
              .toArray(CompletableFuture[]::new))
          .get();
    } catch (InterruptedException e) {
      logger.error("interrupted while delivering multi-recipient messages", e);
      return Response.serverError().entity("interrupted during delivery").build();
    } catch (CancellationException e) {
      logger.error("cancelled while delivering multi-recipient messages", e);
      return Response.serverError().entity("delivery cancelled").build();
    } catch (ExecutionException e) {
      logger.error("partial failure while delivering multi-recipient messages", e.getCause());
      return Response.serverError().entity("failure during delivery").build();
    }
    return Response.ok(new SendMultiRecipientMessageResponse(uuids404)).build();
  }

  private void checkGroupSendCredential(
      final Collection<ServiceId> recipients,
      final Collection<ServiceId> excludedRecipients,
      final @NotNull GroupSendCredentialHeader groupSendCredential) {
    try {
      // A group send credential covers *every* group member except the sender. However, clients
      // don't always want to actually send to every recipient in the same multi-send (most
      // commonly because a new member needs an SKDM first, but also could be because the sender
      // has blocked someone). So we check the group send credential against the combination of
      // the actual recipients and the supplied list of "excluded" recipients, accounts the
      // sender knows are part of the credential but doesn't want to send to right now.
      groupSendCredential.presentation().verify(
          Lists.newArrayList(Iterables.concat(recipients, excludedRecipients)),
          serverSecretParams);
    } catch (VerificationFailedException e) {
      throw new NotAuthorizedException(e);
    }
  }

  private void checkAccessKeys(
      final @NotNull CombinedUnidentifiedSenderAccessKeys accessKeys,
      final Collection<MultiRecipientDeliveryData> destinations) {
    final int keyLength = UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH;
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
  public CompletableFuture<OutgoingMessageEntityList> getPendingMessages(@ReadOnly @Auth AuthenticatedAccount auth,
      @HeaderParam(Stories.X_SIGNAL_RECEIVE_STORIES) String receiveStoriesHeader,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent) {

    boolean shouldReceiveStories = Stories.parseReceiveStoriesHeader(receiveStoriesHeader);

    pushNotificationManager.handleMessagesRetrieved(auth.getAccount(), auth.getAuthenticatedDevice(), userAgent);

    return messagesManager.getMessagesForDevice(
            auth.getAccount().getUuid(),
            auth.getAuthenticatedDevice().getId(),
            false)
        .map(messagesAndHasMore -> {
          Stream<Envelope> envelopes = messagesAndHasMore.first().stream();
          if (!shouldReceiveStories) {
            envelopes = envelopes.filter(e -> !e.getStory());
          }

          final OutgoingMessageEntityList messages = new OutgoingMessageEntityList(envelopes
              .map(OutgoingMessageEntity::fromEnvelope)
              .peek(outgoingMessageEntity -> {
                MessageMetrics.measureAccountOutgoingMessageUuidMismatches(auth.getAccount(), outgoingMessageEntity);
                MessageMetrics.measureOutgoingMessageLatency(outgoingMessageEntity.serverTimestamp(), "rest", userAgent, clientReleaseManager);
              })
              .collect(Collectors.toList()),
              messagesAndHasMore.second());

          Metrics.summary(OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent)))
              .record(estimateMessageListSizeBytes(messages));

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
  public CompletableFuture<Response> removePendingMessage(@ReadOnly @Auth AuthenticatedAccount auth, @PathParam("uuid") UUID uuid) {
    return messagesManager.delete(
            auth.getAccount().getUuid(),
            auth.getAuthenticatedDevice().getId(),
            uuid,
            null)
        .thenAccept(maybeDeletedMessage -> {
          maybeDeletedMessage.ifPresent(deletedMessage -> {

            WebSocketConnection.recordMessageDeliveryDuration(deletedMessage.getTimestamp(),
                auth.getAuthenticatedDevice());

            if (deletedMessage.hasSourceUuid() && deletedMessage.getType() != Type.SERVER_DELIVERY_RECEIPT) {
              try {
                receiptSender.sendReceipt(
                    ServiceIdentifier.valueOf(deletedMessage.getDestinationUuid()), auth.getAuthenticatedDevice().getId(),
                    AciServiceIdentifier.valueOf(deletedMessage.getSourceUuid()), deletedMessage.getTimestamp());
              } catch (Exception e) {
                logger.warn("Failed to send delivery receipt", e);
              }
            }
          });
        })
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  @Timed
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/report/{source}/{messageGuid}")
  public Response reportSpamMessage(
      @ReadOnly @Auth AuthenticatedAccount auth,
      @PathParam("source") String source,
      @PathParam("messageGuid") UUID messageGuid,
      @Nullable @Valid SpamReport spamReport,
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

    // spam report token is optional, but if provided ensure it is valid base64 and non-empty.
    final Optional<byte[]> maybeSpamReportToken =
        Optional.ofNullable(spamReport)
            .flatMap(r -> Optional.ofNullable(r.token()))
            .filter(t -> t.length > 0);

    reportMessageManager.report(sourceNumber, sourceAci, sourcePni, messageGuid, spamReporterUuid, maybeSpamReportToken, userAgent);

    return Response.status(Status.ACCEPTED)
        .build();
  }

  private void sendIndividualMessage(
      Optional<AuthenticatedAccount> source,
      Account destinationAccount,
      Device destinationDevice,
      ServiceIdentifier destinationIdentifier,
      long timestamp,
      boolean online,
      boolean story,
      boolean urgent,
      IncomingMessage incomingMessage,
      String userAgentString,
      Optional<byte[]> spamReportToken)
      throws NoSuchUserException {
    try {
      final Envelope envelope;

      try {
        Account sourceAccount = source.map(AuthenticatedAccount::getAccount).orElse(null);
        Byte sourceDeviceId = source.map(account -> account.getAuthenticatedDevice().getId()).orElse(null);
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
    } catch (NotPushRegisteredException e) {
      if (destinationDevice.isPrimary()) throw new NoSuchUserException(e);
      else                              logger.debug("Not registered", e);
    }
  }

  private void sendCommonPayloadMessage(Account destinationAccount,
      Device destinationDevice,
      ServiceIdentifier serviceIdentifier,
      long timestamp,
      boolean online,
      boolean story,
      boolean urgent,
      byte[] payload) throws NoSuchUserException {
    try {
      Envelope.Builder messageBuilder = Envelope.newBuilder();
      long serverTimestamp = System.currentTimeMillis();

      messageBuilder
          .setType(Type.UNIDENTIFIED_SENDER)
          .setTimestamp(timestamp == 0 ? serverTimestamp : timestamp)
          .setServerTimestamp(serverTimestamp)
          .setContent(ByteString.copyFrom(payload))
          .setStory(story)
          .setUrgent(urgent)
          .setDestinationUuid(serviceIdentifier.toServiceIdentifierString());

      messageSender.sendMessage(destinationAccount, destinationDevice, messageBuilder.build(), online);
    } catch (NotPushRegisteredException e) {
      if (destinationDevice.isPrimary()) {
        throw new NoSuchUserException(e);
      } else {
        logger.debug("Not registered", e);
      }
    }
  }

  private void checkMessageRateLimit(AuthenticatedAccount source, Account destination, String userAgent)
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

  private void validateContentLength(final int contentLength, final String userAgent) {
    Metrics.summary(CONTENT_SIZE_DISTRIBUTION_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent)))
        .record(contentLength);

    if (contentLength > MAX_MESSAGE_SIZE) {
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
