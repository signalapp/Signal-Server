/**
 * Copyright (C) 2015 Open Whisper Systems
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
package org.whispersystems.gcm.server.internal;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class GcmRequestEntity {

  @JsonProperty(value = "collapse_key")
  private String collapseKey;

  @JsonProperty(value = "time_to_live")
  private Long ttl;

  @JsonProperty(value = "delay_while_idle")
  private Boolean delayWhileIdle;

  @JsonProperty(value = "data")
  private Map<String, String> data;

  @JsonProperty(value = "registration_ids")
  private List<String> registrationIds;

  @JsonProperty
  private String priority;

  public GcmRequestEntity(String collapseKey, Long ttl, Boolean delayWhileIdle,
                          Map<String, String> data, List<String> registrationIds,
                          String priority)
  {
    this.collapseKey     = collapseKey;
    this.ttl             = ttl;
    this.delayWhileIdle  = delayWhileIdle;
    this.data            = data;
    this.registrationIds = registrationIds;
    this.priority        = priority;
  }
}
