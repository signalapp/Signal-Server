/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.validation.Valid;

public record SetKeysRequest(
    @Valid
    @Schema(description = """
        A list of unsigned elliptic-curve prekeys to use for this device. If present and not empty, replaces all stored
        unsigned EC prekeys for the device; if absent or empty, any stored unsigned EC prekeys for the device are not
        deleted.
        """)
    List<@Valid ECPreKey> preKeys,

    @Valid
    @Schema(description = """
        An optional signed elliptic-curve prekey to use for this device. If present, replaces the stored signed
        elliptic-curve prekey for the device; if absent, the stored signed prekey is not deleted. If present, must have
        a valid signature from the identity key in this request.
        """)
    ECSignedPreKey signedPreKey,

    @Valid
    @Schema(description = """
        A list of signed post-quantum one-time prekeys to use for this device. Each key must have a valid signature from
        the identity key in this request. If present and not empty, replaces all stored unsigned PQ prekeys for the
        device; if absent or empty, any stored unsigned PQ prekeys for the device are not deleted.
        """)
    List<@Valid KEMSignedPreKey> pqPreKeys,

    @Valid
    @Schema(description = """
        An optional signed last-resort post-quantum prekey to use for this device. If present, replaces the stored
        signed post-quantum last-resort prekey for the device; if absent, a stored last-resort prekey will *not* be
        deleted. If present, must have a valid signature from the identity key in this request.
        """)
    KEMSignedPreKey pqLastResortPreKey) {
}
