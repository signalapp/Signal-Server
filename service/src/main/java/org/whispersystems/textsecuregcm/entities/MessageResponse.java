/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MessageResponse {
  private List<String> success;
  private List<String> failure;
  private Set<String>  missingDeviceIds;

  public MessageResponse(List<String> success, List<String> failure) {
    this.success          = success;
    this.failure          = failure;
    this.missingDeviceIds = new HashSet<>();
  }

  public MessageResponse(Set<String> missingDeviceIds) {
    this.success          = new LinkedList<>();
    this.failure          = new LinkedList<>(missingDeviceIds);
    this.missingDeviceIds = missingDeviceIds;
  }

  public MessageResponse() {}

  public List<String> getSuccess() {
    return success;
  }

  public void setSuccess(List<String> success) {
    this.success = success;
  }

  public List<String> getFailure() {
    return failure;
  }

  public void setFailure(List<String> failure) {
    this.failure = failure;
  }

  public Set<String> getNumbersMissingDevices() {
    return missingDeviceIds;
  }

  public void setNumbersMissingDevices(Set<String> numbersMissingDevices) {
    this.missingDeviceIds = numbersMissingDevices;
  }
}
