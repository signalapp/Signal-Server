/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.StatusException;

public interface FieldValidator {

  void validate(Object extensionValue, Descriptors.FieldDescriptor fd, Message msg)
      throws StatusException;
}
