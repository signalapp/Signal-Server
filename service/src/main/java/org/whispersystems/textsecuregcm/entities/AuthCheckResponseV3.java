/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import javax.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record AuthCheckResponseV3(
    @Schema(description = """
    A dictionary with the auth check results, keyed by the token corresponding token provided in the request.
    """)
    @NotNull Map<String, Result> matches) {

  public record Result(
      @Schema(description = "The status of the credential. Either match, no-match, or invalid")
      CredentialStatus status,

      @Schema(description = """
      If the credential was a match, the stored shareSet that can be used to restore a value from SVR. Encoded in
      standard un-padded base64.
      """, implementation = String.class)
      @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @Nullable byte[] shareSet) {

    public static Result invalid() {
      return new Result(CredentialStatus.INVALID, null);
    }

    public static Result noMatch() {
      return new Result(CredentialStatus.NO_MATCH, null);
    }

    public static Result match(@Nullable final byte[] shareSet) {
      return new Result(CredentialStatus.MATCH, shareSet);
    }
  }

  public enum CredentialStatus {
    MATCH("match"),
    NO_MATCH("no-match"),
    INVALID("invalid");

    private final String clientCode;

    CredentialStatus(final String clientCode) {
      this.clientCode = clientCode;
    }

    @JsonValue
    public String clientCode() {
      return clientCode;
    }
  }
}
