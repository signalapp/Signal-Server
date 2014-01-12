/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
