package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.whispersystems.dropwizard.simpleauth.AuthValueFactoryProvider;
import org.whispersystems.textsecuregcm.configuration.ProfilesConfiguration;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.*;

public class ProfileControllerTest {

  private static AccountsManager       accountsManager = mock(AccountsManager.class      );
  private static RateLimiters          rateLimiters    = mock(RateLimiters.class         );
  private static RateLimiter           rateLimiter     = mock(RateLimiter.class          );
  private static ProfilesConfiguration configuration   = mock(ProfilesConfiguration.class);

  static {
    when(configuration.getAccessKey()).thenReturn("accessKey");
    when(configuration.getAccessSecret()).thenReturn("accessSecret");
    when(configuration.getRegion()).thenReturn("us-east-1");
    when(configuration.getBucket()).thenReturn("profile-bucket");
  }

  @ClassRule
  public static final ResourceTestRule resources = ResourceTestRule.builder()
                                                                   .addProvider(AuthHelper.getAuthFilter())
                                                                   .addProvider(new AuthValueFactoryProvider.Binder())
                                                                   .setMapper(SystemMapper.getMapper())
                                                                   .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                                   .addResource(new ProfileController(rateLimiters,
                                                                                                      accountsManager,
                                                                                                      configuration))
                                                                   .build();

  @Before
  public void setup() throws Exception {
    when(rateLimiters.getProfileLimiter()).thenReturn(rateLimiter);

    Account profileAccount = mock(Account.class);

    when(profileAccount.getIdentityKey()).thenReturn("bar");
    when(profileAccount.getName()).thenReturn("baz");
    when(profileAccount.getAvatar()).thenReturn("profiles/bang");
    when(profileAccount.getAvatarDigest()).thenReturn("buh");

    when(accountsManager.get(AuthHelper.VALID_NUMBER_TWO)).thenReturn(Optional.of(profileAccount));
  }


  @Test
  public void testProfileGet() throws RateLimitExceededException {
    Profile profile= resources.getJerseyTest()
                              .target("/v1/profile/" + AuthHelper.VALID_NUMBER_TWO)
                              .request()
                              .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                              .get(Profile.class);

    assertThat(profile.getIdentityKey()).isEqualTo("bar");
    assertThat(profile.getName()).isEqualTo("baz");
    assertThat(profile.getAvatar()).isEqualTo("profiles/bang");

    verify(accountsManager, times(1)).get(AuthHelper.VALID_NUMBER_TWO);
    verify(rateLimiters, times(1)).getProfileLimiter();
    verify(rateLimiter, times(1)).validate(AuthHelper.VALID_NUMBER);
  }


}
