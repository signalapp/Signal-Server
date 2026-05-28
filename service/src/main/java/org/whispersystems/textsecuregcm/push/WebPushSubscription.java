/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.P256ECPublicKeyAdapter;
import org.whispersystems.textsecuregcm.util.ValidHttpsURI;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.security.interfaces.ECPublicKey;

public record WebPushSubscription(
                                @NotNull
                                @ValidHttpsURI
                                @JsonProperty(required = true)
                                URI endpoint,
                                @NotNull
                                @JsonProperty(value = "publicKey", required = true)
                                @JsonSerialize(using = P256ECPublicKeyAdapter.Serializer.class)
                                @JsonDeserialize(using = P256ECPublicKeyAdapter.Deserializer.class)
                                ECPublicKey userPublicKey,
                                @NotNull
                                @ExactlySize(16)
                                @JsonProperty(value = "auth", required = true)
                                @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
                                byte[] userAuth) {}
