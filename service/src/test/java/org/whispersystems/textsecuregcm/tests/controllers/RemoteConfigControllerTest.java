package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.controllers.RemoteConfigController;
import org.whispersystems.textsecuregcm.entities.UserRemoteConfigList;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.*;

public class RemoteConfigControllerTest {

  private RemoteConfigsManager remoteConfigsManager = mock(RemoteConfigsManager.class);
  private List<String> remoteConfigsAuth = new LinkedList<>() {{
    add("foo");
    add("bar");
  }};

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addProvider(new DeviceLimitExceededExceptionMapper())
                                                            .addResource(new RemoteConfigController(remoteConfigsManager, remoteConfigsAuth))
                                                            .build();


  @Before
  public void setup() throws Exception {
    when(remoteConfigsManager.getAll()).thenReturn(new LinkedList<>() {{
      add(new RemoteConfig("android.stickers", 25, new HashSet<>() {{
        add(AuthHelper.DISABLED_UUID);
        add(AuthHelper.INVALID_UUID);
      }}));

      add(new RemoteConfig("ios.stickers", 50, new HashSet<>() {{

      }}));

      add(new RemoteConfig("always.true", 100, new HashSet<>() {{

      }}));

      add(new RemoteConfig("only.special", 0, new HashSet<>() {{
        add(AuthHelper.VALID_UUID);
      }}));
    }});
  }

  @Test
  public void testRetrieveConfig() {
    UserRemoteConfigList configuration = resources.getJerseyTest()
                                                  .target("/v1/config/")
                                                  .request()
                                                  .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                                  .get(UserRemoteConfigList.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.getConfig().size()).isEqualTo(4);
    assertThat(configuration.getConfig().get(0).getName()).isEqualTo("android.stickers");
    assertThat(configuration.getConfig().get(1).getName()).isEqualTo("ios.stickers");
    assertThat(configuration.getConfig().get(2).getName()).isEqualTo("always.true");
    assertThat(configuration.getConfig().get(2).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(3).isEnabled()).isEqualTo(true);
  }

  @Test
  public void testRetrieveConfigNotSpecial() {
    UserRemoteConfigList configuration = resources.getJerseyTest()
                                                  .target("/v1/config/")
                                                  .request()
                                                  .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER_TWO, AuthHelper.VALID_PASSWORD_TWO))
                                                  .get(UserRemoteConfigList.class);

    verify(remoteConfigsManager, times(1)).getAll();

    assertThat(configuration.getConfig().size()).isEqualTo(4);
    assertThat(configuration.getConfig().get(0).getName()).isEqualTo("android.stickers");
    assertThat(configuration.getConfig().get(1).getName()).isEqualTo("ios.stickers");
    assertThat(configuration.getConfig().get(2).getName()).isEqualTo("always.true");
    assertThat(configuration.getConfig().get(2).isEnabled()).isEqualTo(true);
    assertThat(configuration.getConfig().get(3).isEnabled()).isEqualTo(false);
  }


  @Test
  public void testRetrieveConfigUnauthorized() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config/")
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
                                 .get();

    assertThat(response.getStatus()).isEqualTo(401);

    verifyNoMoreInteractions(remoteConfigsManager);
  }


  @Test
  public void testSetConfig() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config")
                                 .request()
                                 .header("Config-Token", "foo")
                                 .put(Entity.entity(new RemoteConfig("android.stickers", 88, new HashSet<>()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<RemoteConfig> captor = ArgumentCaptor.forClass(RemoteConfig.class);

    verify(remoteConfigsManager, times(1)).set(captor.capture());

    assertThat(captor.getValue().getName()).isEqualTo("android.stickers");
    assertThat(captor.getValue().getPercentage()).isEqualTo(88);
    assertThat(captor.getValue().getUuids()).isEmpty();
  }

  @Test
  public void testSetConfigUnauthorized() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config")
                                 .request()
                                 .header("Config-Token", "baz")
                                 .put(Entity.entity(new RemoteConfig("android.stickers", 88, new HashSet<>()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(401);

    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  public void testSetConfigMissingUnauthorized() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config")
                                 .request()
                                 .put(Entity.entity(new RemoteConfig("android.stickers", 88, new HashSet<>()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(401);

    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  public void testSetConfigBadName() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config")
                                 .request()
                                 .header("Config-Token", "foo")
                                 .put(Entity.entity(new RemoteConfig("android-stickers", 88, new HashSet<>()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);

    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  public void testSetConfigEmptyName() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config")
                                 .request()
                                 .header("Config-Token", "foo")
                                 .put(Entity.entity(new RemoteConfig("", 88, new HashSet<>()), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);

    verifyNoMoreInteractions(remoteConfigsManager);
  }


  @Test
  public void testDelete() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config/android.stickers")
                                 .request()
                                 .header("Config-Token", "foo")
                                 .delete();

    assertThat(response.getStatus()).isEqualTo(204);

    verify(remoteConfigsManager, times(1)).delete("android.stickers");
    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  public void testDeleteUnauthorized() {
    Response response = resources.getJerseyTest()
                                 .target("/v1/config/android.stickers")
                                 .request()
                                 .header("Config-Token", "baz")
                                 .delete();

    assertThat(response.getStatus()).isEqualTo(401);

    verifyNoMoreInteractions(remoteConfigsManager);
  }

  @Test
  public void testMath() throws NoSuchAlgorithmException {
    List<RemoteConfig>   remoteConfigList = remoteConfigsManager.getAll();
    Map<String, Integer> enabledMap       = new HashMap<>();
    MessageDigest        digest           = MessageDigest.getInstance("SHA1");
    int                  iterations       = 100000;

    for (int i=0;i<iterations;i++) {
      for (RemoteConfig config : remoteConfigList) {
        int count  = enabledMap.getOrDefault(config.getName(), 0);
        int random = new SecureRandom().nextInt(iterations);

        if (RemoteConfigController.isInBucket(digest, UUID.randomUUID(), config.getName().getBytes(), config.getPercentage(), new HashSet<>())) {
          count++;
        }

        enabledMap.put(config.getName(), count);
      }
    }

    for (RemoteConfig config : remoteConfigList) {
      double targetNumber = iterations   * (config.getPercentage() / 100.0);
      double variance     = targetNumber * 0.01;

      assertThat(enabledMap.get(config.getName())).isBetween((int)(targetNumber - variance), (int)(targetNumber + variance));
    }

  }


}
