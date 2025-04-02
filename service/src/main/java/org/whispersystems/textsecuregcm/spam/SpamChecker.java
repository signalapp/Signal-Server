/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.spam;

import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.Optional;
import jakarta.ws.rs.core.Response;
import org.signal.chat.messages.SendMessageResponse;
import org.signal.chat.messages.SendMultiRecipientMessageResponse;
import org.whispersystems.textsecuregcm.auth.AccountAndAuthenticatedDeviceHolder;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;

public interface SpamChecker {

  /**
   * Determine if a message sent to an individual recipient via HTTP may be spam.
   *
   * @param requestContext   The request context for a message send attempt
   * @param maybeSource      The sender of the message, could be empty if this as message sent with sealed sender
   * @param maybeDestination The destination of the message, could be empty if the destination does not exist or could
   *                         not be retrieved
   * @return A {@link SpamCheckResult}
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  SpamCheckResult<Response> checkForIndividualRecipientSpamHttp(
      final MessageType messageType,
      final ContainerRequestContext requestContext,
      final Optional<? extends AccountAndAuthenticatedDeviceHolder> maybeSource,
      final Optional<Account> maybeDestination,
      final Optional<ServiceIdentifier> maybeDestinationIdentifier);

  /**
   * Determine if a message sent to multiple recipients via HTTP may be spam.
   *
   * @param requestContext   The request context for a message send attempt
   * @return A {@link SpamCheckResult}
   */
  SpamCheckResult<Response> checkForMultiRecipientSpamHttp(
      final MessageType messageType,
      final ContainerRequestContext requestContext);

  /**
   * Determine if a message sent to an individual recipient via gRPC may be spam.
   *
   * @param maybeSource      The sender of the message, could be empty if this as message sent with sealed sender
   * @param maybeDestination The destination of the message, could be empty if the destination does not exist or could
   *                         not be retrieved
   * @return A {@link SpamCheckResult}
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  SpamCheckResult<GrpcResponse<SendMessageResponse>> checkForIndividualRecipientSpamGrpc(
      final MessageType messageType,
      final Optional<org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice> maybeSource,
      final Optional<Account> maybeDestination,
      final Optional<ServiceIdentifier> maybeDestinationIdentifier);

  /**
   * Determine if a message sent to multiple recipients via gRPC may be spam.
   *
   * @return A {@link SpamCheckResult}
   */
  SpamCheckResult<GrpcResponse<SendMultiRecipientMessageResponse>> checkForMultiRecipientSpamGrpc(final MessageType messageType);


  static SpamChecker noop() {
    return new SpamChecker() {

      @Override
      public SpamCheckResult<Response> checkForIndividualRecipientSpamHttp(final MessageType messageType,
          final ContainerRequestContext requestContext,
          final Optional<? extends AccountAndAuthenticatedDeviceHolder> maybeSource,
          final Optional<Account> maybeDestination,
          final Optional<ServiceIdentifier> maybeDestinationIdentifier) {

        return new SpamCheckResult<>(Optional.empty(), Optional.empty());
      }

      @Override
      public SpamCheckResult<Response> checkForMultiRecipientSpamHttp(final MessageType messageType,
          final ContainerRequestContext requestContext) {

        return new SpamCheckResult<>(Optional.empty(), Optional.empty());
      }

      @Override
      public SpamCheckResult<GrpcResponse<SendMessageResponse>> checkForIndividualRecipientSpamGrpc(final MessageType messageType,
          final Optional<AuthenticatedDevice> maybeSource,
          final Optional<Account> maybeDestination,
          final Optional<ServiceIdentifier> maybeDestinationIdentifier) {

        return new SpamCheckResult<>(Optional.empty(), Optional.empty());
      }

      @Override
      public SpamCheckResult<GrpcResponse<SendMultiRecipientMessageResponse>> checkForMultiRecipientSpamGrpc(
          final MessageType messageType) {

        return new SpamCheckResult<>(Optional.empty(), Optional.empty());
      }
    };
  }
}
