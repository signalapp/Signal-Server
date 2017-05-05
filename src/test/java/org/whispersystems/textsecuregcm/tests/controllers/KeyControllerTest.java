package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.dropwizard.simpleauth.AuthValueFactoryProvider;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeyRecord;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class KeyControllerTest {

  private static final String EXISTS_NUMBER     = "+14152222222";
  private static String NOT_EXISTS_NUMBER = "+14152222220";

  private static int SAMPLE_REGISTRATION_ID  =  999;
  private static int SAMPLE_REGISTRATION_ID2 = 1002;
  private static int SAMPLE_REGISTRATION_ID4 = 1555;

  private final KeyRecord SAMPLE_KEY    = new KeyRecord(1, EXISTS_NUMBER, Device.MASTER_ID, 1234, "test1", false);
  private final KeyRecord SAMPLE_KEY2   = new KeyRecord(2, EXISTS_NUMBER, 2, 5667, "test3", false               );
  private final KeyRecord SAMPLE_KEY3   = new KeyRecord(3, EXISTS_NUMBER, 3, 334, "test5", false                );
  private final KeyRecord SAMPLE_KEY4   = new KeyRecord(4, EXISTS_NUMBER, 4, 336, "test6", false                );


  private final SignedPreKey SAMPLE_SIGNED_KEY  = new SignedPreKey(1111, "foofoo", "sig11");
  private final SignedPreKey SAMPLE_SIGNED_KEY2 = new SignedPreKey(2222, "foobar", "sig22");
  private final SignedPreKey SAMPLE_SIGNED_KEY3 = new SignedPreKey(3333, "barfoo", "sig33");

  private final Keys            keys          = mock(Keys.class           );
  private final AccountsManager accounts      = mock(AccountsManager.class);
  private final Account         existsAccount = mock(Account.class        );

  private RateLimiters          rateLimiters  = mock(RateLimiters.class);
  private RateLimiter           rateLimiter   = mock(RateLimiter.class );

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new AuthValueFactoryProvider.Binder())
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new KeysController(rateLimiters, keys, accounts, null))
                                                            .build();

  @Before
  public void setup() {
    final Device sampleDevice  = mock(Device.class);
    final Device sampleDevice2 = mock(Device.class);
    final Device sampleDevice3 = mock(Device.class);
    final Device sampleDevice4 = mock(Device.class);

    Set<Device> allDevices = new HashSet<Device>() {{
      add(sampleDevice);
      add(sampleDevice2);
      add(sampleDevice3);
      add(sampleDevice4);
    }};

    when(sampleDevice.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID);
    when(sampleDevice2.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID2);
    when(sampleDevice3.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID2);
    when(sampleDevice4.getRegistrationId()).thenReturn(SAMPLE_REGISTRATION_ID4);
    when(sampleDevice.isActive()).thenReturn(true);
    when(sampleDevice2.isActive()).thenReturn(true);
    when(sampleDevice3.isActive()).thenReturn(false);
    when(sampleDevice4.isActive()).thenReturn(true);
    when(sampleDevice.getSignedPreKey()).thenReturn(SAMPLE_SIGNED_KEY);
    when(sampleDevice2.getSignedPreKey()).thenReturn(SAMPLE_SIGNED_KEY2);
    when(sampleDevice3.getSignedPreKey()).thenReturn(SAMPLE_SIGNED_KEY3);
    when(sampleDevice4.getSignedPreKey()).thenReturn(null);
    when(sampleDevice.getId()).thenReturn(1L);
    when(sampleDevice2.getId()).thenReturn(2L);
    when(sampleDevice3.getId()).thenReturn(3L);
    when(sampleDevice4.getId()).thenReturn(4L);

    when(existsAccount.getDevice(1L)).thenReturn(Optional.of(sampleDevice));
    when(existsAccount.getDevice(2L)).thenReturn(Optional.of(sampleDevice2));
    when(existsAccount.getDevice(3L)).thenReturn(Optional.of(sampleDevice3));
    when(existsAccount.getDevice(4L)).thenReturn(Optional.of(sampleDevice4));
    when(existsAccount.getDevice(22L)).thenReturn(Optional.<Device>absent());
    when(existsAccount.getDevices()).thenReturn(allDevices);
    when(existsAccount.isActive()).thenReturn(true);
    when(existsAccount.getIdentityKey()).thenReturn("existsidentitykey");
    when(existsAccount.getNumber()).thenReturn(EXISTS_NUMBER);

    when(accounts.get(EXISTS_NUMBER)).thenReturn(Optional.of(existsAccount));
    when(accounts.get(NOT_EXISTS_NUMBER)).thenReturn(Optional.<Account>absent());

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    List<KeyRecord> singleDevice = new LinkedList<>();
    singleDevice.add(SAMPLE_KEY);
    when(keys.get(eq(EXISTS_NUMBER), eq(1L))).thenReturn(Optional.of(singleDevice));

    when(keys.get(eq(NOT_EXISTS_NUMBER), eq(1L))).thenReturn(Optional.<List<KeyRecord>>absent());

    List<KeyRecord> multiDevice = new LinkedList<>();
    multiDevice.add(SAMPLE_KEY);
    multiDevice.add(SAMPLE_KEY2);
    multiDevice.add(SAMPLE_KEY3);
    multiDevice.add(SAMPLE_KEY4);
    when(keys.get(EXISTS_NUMBER)).thenReturn(Optional.of(multiDevice));

    when(keys.getCount(eq(AuthHelper.VALID_NUMBER), eq(1L))).thenReturn(5);

    when(AuthHelper.VALID_DEVICE.getSignedPreKey()).thenReturn(new SignedPreKey(89898, "zoofarb", "sigvalid"));
    when(AuthHelper.VALID_ACCOUNT.getIdentityKey()).thenReturn(null);
  }

  @Test
  public void validKeyStatusTestV2() throws Exception {
    PreKeyCount result = resources.getJerseyTest()
                                  .target("/v2/keys")
                                  .request()
                                  .header("Authorization",
                                          AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                  .get(PreKeyCount.class);

    assertThat(result.getCount() == 4);

    verify(keys).getCount(eq(AuthHelper.VALID_NUMBER), eq(1L));
  }

  @Test
  public void getSignedPreKeyV2() throws Exception {
    SignedPreKey result = resources.getJerseyTest()
                                   .target("/v2/keys/signed")
                                   .request()
                                   .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                   .get(SignedPreKey.class);

    assertThat(result.equals(SAMPLE_SIGNED_KEY));
  }

  @Test
  public void putSignedPreKeyV2() throws Exception {
    SignedPreKey   test     = new SignedPreKey(9999, "fooozzz", "baaarzzz");
    Response response = resources.getJerseyTest()
                                       .target("/v2/keys/signed")
                                       .request()
                                       .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                       .put(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus() == 204);

    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(test));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT));
  }

  @Test
  public void validSingleRequestTestV2() throws Exception {
    PreKeyResponse result = resources.getJerseyTest()
                                     .target(String.format("/v2/keys/%s/1", EXISTS_NUMBER))
                                     .request()
                                     .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                     .get(PreKeyResponse.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevicesCount()).isEqualTo(1);
    assertThat(result.getDevice(1).getPreKey().getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getDevice(1).getPreKey().getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getDevice(1).getSignedPreKey()).isEqualTo(existsAccount.getDevice(1).get().getSignedPreKey());

    verify(keys).get(eq(EXISTS_NUMBER), eq(1L));
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void validMultiRequestTestV2() throws Exception {
    PreKeyResponse results = resources.getJerseyTest()
                                      .target(String.format("/v2/keys/%s/*", EXISTS_NUMBER))
                                      .request()
                                      .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                      .get(PreKeyResponse.class);

    assertThat(results.getDevicesCount()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());

    PreKey signedPreKey   = results.getDevice(1).getSignedPreKey();
    PreKey preKey         = results.getDevice(1).getPreKey();
    long     registrationId = results.getDevice(1).getRegistrationId();
    long     deviceId       = results.getDevice(1).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertThat(signedPreKey.getKeyId()).isEqualTo(SAMPLE_SIGNED_KEY.getKeyId());
    assertThat(signedPreKey.getPublicKey()).isEqualTo(SAMPLE_SIGNED_KEY.getPublicKey());
    assertThat(deviceId).isEqualTo(1);

    signedPreKey   = results.getDevice(2).getSignedPreKey();
    preKey         = results.getDevice(2).getPreKey();
    registrationId = results.getDevice(2).getRegistrationId();
    deviceId       = results.getDevice(2).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY2.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY2.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertThat(signedPreKey.getKeyId()).isEqualTo(SAMPLE_SIGNED_KEY2.getKeyId());
    assertThat(signedPreKey.getPublicKey()).isEqualTo(SAMPLE_SIGNED_KEY2.getPublicKey());
    assertThat(deviceId).isEqualTo(2);

    signedPreKey   = results.getDevice(4).getSignedPreKey();
    preKey         = results.getDevice(4).getPreKey();
    registrationId = results.getDevice(4).getRegistrationId();
    deviceId       = results.getDevice(4).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY4.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY4.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID4);
    assertThat(signedPreKey).isNull();
    assertThat(deviceId).isEqualTo(4);

    verify(keys).get(eq(EXISTS_NUMBER));
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void invalidRequestTestV2() throws Exception {
    Response response = resources.getJerseyTest()
                                 .target(String.format("/v2/keys/%s", NOT_EXISTS_NUMBER))
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  public void anotherInvalidRequestTestV2() throws Exception {
    Response response = resources.getJerseyTest()
                                 .target(String.format("/v2/keys/%s/22", EXISTS_NUMBER))
                                 .request()
                                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  public void unauthorizedRequestTestV2() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v2/keys/%s/1", EXISTS_NUMBER))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);

    response =
        resources.getJerseyTest()
                 .target(String.format("/v2/keys/%s/1", EXISTS_NUMBER))
                 .request()
                 .get();

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
  }

  @Test
  public void putKeysTestV2() throws Exception {
    final PreKey preKey        = new PreKey(31337, "foobar");
    final PreKey lastResortKey = new PreKey(31339, "barbar");
    final SignedPreKey signedPreKey  = new SignedPreKey(31338, "foobaz", "myvalidsig");
    final String       identityKey   = "barbar";

    List<PreKey> preKeys = new LinkedList<PreKey>() {{
      add(preKey);
    }};

    PreKeyState preKeyState = new PreKeyState(identityKey, signedPreKey, preKeys);

    Response response =
        resources.getJerseyTest()
                 .target("/v2/keys")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.entity(preKeyState, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(keys).store(eq(AuthHelper.VALID_NUMBER), eq(1L), listCaptor.capture());

    List<PreKey> capturedList = listCaptor.getValue();
    assertThat(capturedList.size() == 1);
    assertThat(capturedList.get(0).getKeyId() == 31337);
    assertThat(capturedList.get(0).getPublicKey().equals("foobar"));

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq("barbar"));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(AuthHelper.VALID_ACCOUNT);
  }


}