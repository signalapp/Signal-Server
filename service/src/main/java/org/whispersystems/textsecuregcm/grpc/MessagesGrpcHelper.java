/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Map;
import java.util.Optional;
import org.signal.chat.messages.MismatchedDevices;
import org.signal.chat.messages.SendMessageResponse;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.storage.Account;

public class MessagesGrpcHelper {

  private static final SendMessageResponse SEND_MESSAGE_SUCCESS_RESPONSE = SendMessageResponse.newBuilder().build();

  /**
   * Sends a "bundle" of messages to an individual destination account, mapping common exceptions to appropriate gRPC
   * statuses.
   *
   * @param messageSender the {@code MessageSender} instance to use to send the messages
   * @param destination the destination account for the messages
   * @param destinationServiceIdentifier the service identifier for the destination account
   * @param messagesByDeviceId a map of device IDs to message payloads
   * @param registrationIdsByDeviceId a map of device IDs to device registration IDs
   * @param syncMessageSenderDeviceId if the message is a sync message (i.e. a message to other devices linked to the
   *                                  caller's own account), contains the ID of the device that sent the message
   *
   * @return a response object to send to callers
   *
   * @throws StatusException if the message bundle could not be sent due to an out-of-date device set or an invalid
   * message payload
   * @throws RateLimitExceededException if the message bundle could not be sent due to a violated rated limit
   */
  public static SendMessageResponse sendMessage(final MessageSender messageSender,
      final Account destination,
      final ServiceIdentifier destinationServiceIdentifier,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId,
      final Map<Byte, Integer> registrationIdsByDeviceId,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Byte> syncMessageSenderDeviceId)
      throws StatusException, RateLimitExceededException {

    try {
      messageSender.sendMessages(destination,
          destinationServiceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          syncMessageSenderDeviceId,
          RequestAttributesUtil.getUserAgent().orElse(null));

      return SEND_MESSAGE_SUCCESS_RESPONSE;
    } catch (final MismatchedDevicesException e) {
      return SendMessageResponse.newBuilder()
          .setMismatchedDevices(buildMismatchedDevices(destinationServiceIdentifier, e.getMismatchedDevices()))
          .build();
    } catch (final MessageTooLargeException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Message too large").withCause(e).asException();
    }
  }

  /**
   * Translates an internal {@link org.whispersystems.textsecuregcm.controllers.MismatchedDevices} entity to a gRPC
   * {@link MismatchedDevices} entity.
   *
   * @param serviceIdentifier the service identifier to which the mismatched device response applies
   * @param mismatchedDevices the mismatched device entity to translate to gRPC
   *
   * @return a gRPC {@code MismatchedDevices} representation of the given mismatched devices
   */
  public static MismatchedDevices buildMismatchedDevices(final ServiceIdentifier serviceIdentifier,
      final org.whispersystems.textsecuregcm.controllers.MismatchedDevices mismatchedDevices) {

    final MismatchedDevices.Builder mismatchedDevicesBuilder = MismatchedDevices.newBuilder()
        .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier));

    mismatchedDevices.missingDeviceIds().forEach(mismatchedDevicesBuilder::addMissingDevices);
    mismatchedDevices.extraDeviceIds().forEach(mismatchedDevicesBuilder::addExtraDevices);
    mismatchedDevices.staleDeviceIds().forEach(mismatchedDevicesBuilder::addStaleDevices);

    return mismatchedDevicesBuilder.build();
  }
}
