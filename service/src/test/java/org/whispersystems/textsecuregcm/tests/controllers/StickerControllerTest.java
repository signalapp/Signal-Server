package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.controllers.StickerController;
import org.whispersystems.textsecuregcm.entities.StickerPackFormUploadAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import javax.ws.rs.core.Response;
import java.io.IOException;

import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.*;

public class StickerControllerTest {

  private static RateLimiter  rateLimiter  = mock(RateLimiter.class );
  private static RateLimiters rateLimiters = mock(RateLimiters.class);

  @ClassRule
  public static final ResourceTestRule resources = ResourceTestRule.builder()
                                                                   .addProvider(AuthHelper.getAuthFilter())
                                                                   .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                                   .setMapper(SystemMapper.getMapper())
                                                                   .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                                   .addResource(new StickerController(rateLimiters, "foo", "bar", "us-east-1", "mybucket"))
                                                                   .build();

  @Before
  public void setup() {
    when(rateLimiters.getStickerPackLimiter()).thenReturn(rateLimiter);
  }

  @Test
  public void testCreatePack() throws RateLimitExceededException, IOException {
    StickerPackFormUploadAttributes attributes  = resources.getJerseyTest()
                                                           .target("/v1/sticker/pack/form/10")
                                                           .request()
                                                           .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                                           .get(StickerPackFormUploadAttributes.class);

    assertThat(attributes.getPackId()).isNotNull();
    assertThat(attributes.getPackId().length()).isEqualTo(32);

    assertThat(attributes.getManifest()).isNotNull();
    assertThat(attributes.getManifest().getKey()).isEqualTo("stickers/" + attributes.getPackId() + "/manifest.proto");
    assertThat(attributes.getManifest().getAcl()).isEqualTo("private");
    assertThat(attributes.getManifest().getPolicy()).isNotEmpty();
    assertThat(new String(Base64.decode(attributes.getManifest().getPolicy()))).contains("[\"content-length-range\", 1, 10240]");
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
      assertThat(new String(Base64.decode(attributes.getStickers().get(i).getPolicy()))).contains("[\"content-length-range\", 1, 100155]");
      assertThat(attributes.getStickers().get(i).getSignature()).isNotEmpty();
      assertThat(attributes.getStickers().get(i).getAlgorithm()).isEqualTo("AWS4-HMAC-SHA256");
      assertThat(attributes.getStickers().get(i).getCredential()).isNotEmpty();
    }

    verify(rateLimiters, times(1)).getStickerPackLimiter();
    verify(rateLimiter, times(1)).validate(eq(AuthHelper.VALID_NUMBER));
  }

  @Test
  public void testCreateTooLargePack() throws Exception {
    Response response = resources.getJerseyTest()
                        .target("/v1/sticker/pack/form/202")
                        .request()
                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                        .get();

    assertThat(response.getStatus()).isEqualTo(400);

  }

}
