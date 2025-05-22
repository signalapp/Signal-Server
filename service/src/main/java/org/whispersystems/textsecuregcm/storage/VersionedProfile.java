/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;

public record VersionedProfile (String version,
                                @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                byte[] name,

                                @Nullable
                                String avatar,

                                @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                byte[] aboutEmoji,

                                @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                byte[] about,

                                @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                                byte[] paymentAddress,

                                @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                byte[] phoneNumberSharing,

                                @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                                @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                byte[] commitment) {}
