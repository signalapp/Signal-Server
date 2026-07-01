/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.common.DeviceCapability;
import org.signal.chat.profile.ProfileResult;
import org.signal.chat.profile.AccountInfo;

class ProfileGrpcHelperTest {

  public static Stream<Arguments> etagTestVectors() {
    // Expected values of the etag calculation for example profile data sets. A failure here indicates a potential
    // backwards compatability issue.
    return Stream.of(
        Arguments.argumentSet("empty", ProfileResult.newBuilder().setAccountInfo(AccountInfo.getDefaultInstance()).build(), "6NXrB3FC907RoA=="),
        Arguments.argumentSet("all fields set", profile().build(), "zrvx5cC+UDrLzQ=="),
        Arguments.argumentSet("collection ordering", ProfileResult.newBuilder().setAccountInfo(AccountInfo.newBuilder()
                .addBadgeIds("badgeZ").addBadgeIds("badgeQ").addBadgeIds("badgeQQ"))
                .build() , "AS2DN9M/IapIgA=="),
        Arguments.argumentSet("data only", ProfileResult.newBuilder().setData(ByteString.fromHex("abcde123")).build(), "ZKDfIrGEKT1+Sw=="),
        Arguments.argumentSet("payment address only", ProfileResult.newBuilder().setPaymentAddress(ByteString.fromHex("abcde123")).build(), "5CWKoXAq1/cuAg==")
    );
  }

  @ParameterizedTest
  @MethodSource
  void etagTestVectors(final ProfileResult profile, final String expectedEtagB64) {
    final byte[] etag = ProfileGrpcHelper.etag(profile);
    final byte[] expectedEtag = Base64.getDecoder().decode(expectedEtagB64);

    assertEquals(10, etag.length);
    assertArrayEquals(expectedEtag, etag);
  }

  @Test
  void etagIgnoresBadgeOrdering() {
    final byte[] ascending = ProfileGrpcHelper.etag(ProfileResult.newBuilder()
        .setAccountInfo(AccountInfo.newBuilder()
            .addBadgeIds("badge-a")
            .addBadgeIds("badge-b"))
        .build());

    final byte[] descending = ProfileGrpcHelper.etag(ProfileResult.newBuilder()
        .setAccountInfo(AccountInfo.newBuilder()
            .addBadgeIds("badge-b")
            .addBadgeIds("badge-a"))
        .build());

    assertArrayEquals(ascending, descending);
  }

  @Test
  void etagIgnoresEtag() {
    final ProfileResult profile = profile().build();
    final byte[] etag = ProfileGrpcHelper.etag(profile);
    final ProfileResult withEtagSet = profile.toBuilder().setEtag(ByteString.copyFrom(etag)).build();

    assertArrayEquals(etag, ProfileGrpcHelper.etag(withEtagSet));
  }

  @Test
  void absentAndEmptyHaveSameEtag() {
    final byte[] unset = ProfileGrpcHelper.etag(ProfileResult.newBuilder().build());
    final byte[] explicitlyEmpty = ProfileGrpcHelper.etag(ProfileResult.newBuilder()
        .setData(ByteString.EMPTY)
        .setPaymentAddress(ByteString.EMPTY)
        .setAccountInfo(AccountInfo.newBuilder()
            .setIdentityKey(ByteString.EMPTY)
            .setUnidentifiedAccessKeyFingerprint(ByteString.EMPTY)
            .addAllBadgeIds(Collections.emptyList()))
        .build());

    assertArrayEquals(unset, explicitlyEmpty);
  }

  private static ProfileResult.Builder profile() {
    return ProfileResult.newBuilder()
        .setData(ByteString.copyFromUtf8("profile-data"))
        .setPaymentAddress(ByteString.copyFromUtf8("payment-address"))
        .setAccountInfo(AccountInfo.newBuilder()
            .setIdentityKey(ByteString.copyFromUtf8("identity-key"))
            .setUnidentifiedAccessKeyFingerprint(ByteString.copyFromUtf8("uak-checksum"))
            .setUnrestrictedUnidentifiedAccess(true)
            .addBadgeIds("badge-a")
            .addBadgeIds("badge-b"));
  }

}
