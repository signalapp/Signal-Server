/*
 * Copyright (C) 2019 Open WhisperSystems
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

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Size;
import javax.validation.valueextraction.Unwrapping;
import java.util.Optional;

public class DirectoryFeedbackRequest {

  @Size(max = 1024, payload = {Unwrapping.Unwrap.class})
  @JsonProperty
  private Optional<String> reason;

  public DirectoryFeedbackRequest() {
  }

  public DirectoryFeedbackRequest(Optional<String> reason) {
    this.reason = reason;
  }

  public Optional<String> getReason() {
    return reason;
  }

}
