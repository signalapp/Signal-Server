/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.StickerPackFormUploadAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class StickerControllerTest {

  private static final RateLimiter  rateLimiter  = mock(RateLimiter.class );
  private static final RateLimiters rateLimiters = mock(RateLimiters.class);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new StickerController(rateLimiters, "foo", "bar", "us-east-1", "mybucket"))
      .build();

  @BeforeEach
  void setup() {
    when(rateLimiters.getStickerPackLimiter()).thenReturn(rateLimiter);
  }

  @Test
  void testCreatePack() throws RateLimitExceededException {
    StickerPackFormUploadAttributes attributes  = resources.getJerseyTest()
                                                           .target("/v1/sticker/pack/form/10")
                                                           .request()
                                                           .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                                                           .get(StickerPackFormUploadAttributes.class);

    assertThat(attributes.getPackId()).isNotNull();
    assertThat(attributes.getPackId().length()).isEqualTo(32);

    assertThat(attributes.getManifest()).isNotNull();
    assertThat(attributes.getManifest().getKey()).isEqualTo("stickers/" + attributes.getPackId() + "/manifest.proto");
    assertThat(attributes.getManifest().getAcl()).isEqualTo("private");
    assertThat(attributes.getManifest().getPolicy()).isNotEmpty();
    assertThat(new String(Base64.getDecoder().decode(attributes.getManifest().getPolicy()))).contains("[\"content-length-range\", 1, 10240]");
    assertThat(attributes.getManifest().getSignature()).isNotEmpty();
    assertThat(attributes.getManifest().getAlgorithm()).isEqualTo("AWS4-HMAC-SHA256");
    assertThat(attributes.getManifest().getCredential()).isNotEmpty();
    assertThat(attributes.getManifest().getId()).isEqualTo(-1);

    assertThat(attributes.getStickers().size()).isEqualTo(10);

    for (int i=0;i<10;i++) {
      assertThat(attributes.getStickers().get(i).getId()).isEqualTo(i);
      assertThat(attributes.getStickers().get(i).getKey()).isEqualTo("stickers/" + attributes.getPackId() + "/full/" + i);
      assertThat(attributes.getStickers().get(i).getAcl()).isEqualTo("private");
      assertThat(attributes.getStickers().get(i).getPolicy()).isNotEmpty();
      assertThat(new String(Base64.getDecoder().decode(attributes.getStickers().get(i).getPolicy()))).contains("[\"content-length-range\", 1, 308224]");
      assertThat(attributes.getStickers().get(i).getSignature()).isNotEmpty();
      assertThat(attributes.getStickers().get(i).getAlgorithm()).isEqualTo("AWS4-HMAC-SHA256");
      assertThat(attributes.getStickers().get(i).getCredential()).isNotEmpty();
    }

    verify(rateLimiters, times(1)).getStickerPackLimiter();
    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_UUID);
  }

  @Test
  void testCreateTooLargePack() {
    Response response = resources.getJerseyTest()
                        .target("/v1/sticker/pack/form/202")
                        .request()
                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                        .get();

    assertThat(response.getStatus()).isEqualTo(400);
  }

}
