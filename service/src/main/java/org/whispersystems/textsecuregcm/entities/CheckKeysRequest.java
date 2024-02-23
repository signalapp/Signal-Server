package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record CheckKeysRequest(
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
        The identity type for which to check for a shared view of repeated-use keys
        """)
    IdentityType identityType,

    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @ExactlySize(32)
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
        A 32-byte digest of the client's repeated-use keys for the given identity type. The digest is calculated as:
              
        SHA256(identityKeyBytes || signedEcPreKeyId || signedEcPreKeyIdBytes || lastResortKeyId || lastResortKeyBytes)
              
        …where the elements of the hash are:
              
        - identityKeyBytes: the serialized form of the client's public identity key as produced by libsignal (i.e. one
          version byte followed by 32 bytes of key material for a total of 33 bytes)
        - signedEcPreKeyId: an 8-byte, big-endian representation of the ID of the client's signed EC pre-key
        - signedEcPreKeyBytes: the serialized form of the client's signed EC pre-key as produced by libsignal (i.e. one
          version byte followed by 32 bytes of key material for a total of 33 bytes)
        - lastResortKeyId: an 8-byte, big-endian representation of the ID of the client's last-resort Kyber key
        - lastResortKeyBytes: the serialized form of the client's last-resort Kyber key as produced by libsignal (i.e.
          one version byte followed by 1568 bytes of key material for a total of 1569 bytes)
          """)
    byte[] digest) {
}
