package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.controllers.KeysControllerV1;
import org.whispersystems.textsecuregcm.controllers.KeysControllerV2;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV1;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV2;
import org.whispersystems.textsecuregcm.entities.PreKeyStateV1;
import org.whispersystems.textsecuregcm.entities.PreKeyStateV2;
import org.whispersystems.textsecuregcm.entities.PreKeyV1;
import org.whispersystems.textsecuregcm.entities.PreKeyV2;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeyRecord;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.fest.assertions.api.Assertions.assertThat;
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
                                                            .addProvider(AuthHelper.getAuthenticator())
                                                            .addResource(new KeysControllerV1(rateLimiters, keys, accounts, null))
                                                            .addResource(new KeysControllerV2(rateLimiters, keys, accounts, null))
                                                            .build();

  @Before
  public void setup() {
    final Device sampleDevice  = mock(Device.class);
    final Device sampleDevice2 = mock(Device.class);
    final Device sampleDevice3 = mock(Device.class);
    final Device sampleDevice4 = mock(Device.class);

    List<Device> allDevices = new LinkedList<Device>() {{
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
  public void validKeyStatusTestV1() throws Exception {
    PreKeyCount result = resources.client().resource("/v1/keys")
        .header("Authorization",
                AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(PreKeyCount.class);

    assertThat(result.getCount() == 4);

    verify(keys).getCount(eq(AuthHelper.VALID_NUMBER), eq(1L));
  }

  @Test
  public void validKeyStatusTestV2() throws Exception {
    PreKeyCount result = resources.client().resource("/v2/keys")
                                  .header("Authorization",
                                          AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                  .get(PreKeyCount.class);

    assertThat(result.getCount() == 4);

    verify(keys).getCount(eq(AuthHelper.VALID_NUMBER), eq(1L));
  }

  @Test
  public void getSignedPreKeyV2() throws Exception {
    SignedPreKey result = resources.client().resource("/v2/keys/signed")
                                   .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                   .get(SignedPreKey.class);

    assertThat(result.equals(SAMPLE_SIGNED_KEY));
  }

  @Test
  public void putSignedPreKeyV2() throws Exception {
    SignedPreKey   test     = new SignedPreKey(9999, "fooozzz", "baaarzzz");
    ClientResponse response = resources.client().resource("/v2/keys/signed")
                                       .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                       .type(MediaType.APPLICATION_JSON_TYPE)
                                       .put(ClientResponse.class, test);

    assertThat(response.getStatus() == 204);

    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(test));
    verify(accounts).update(eq(AuthHelper.VALID_ACCOUNT));
  }

  @Test
  public void validLegacyRequestTest() throws Exception {
    PreKeyV1 result = resources.client().resource(String.format("/v1/keys/%s", EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(PreKeyV1.class);

    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());

    verify(keys).get(eq(EXISTS_NUMBER), eq(1L));
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void validSingleRequestTestV2() throws Exception {
    PreKeyResponseV2 result = resources.client().resource(String.format("/v2/keys/%s/1", EXISTS_NUMBER))
                                       .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                       .get(PreKeyResponseV2.class);

    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getDevices().size()).isEqualTo(1);
    assertThat(result.getDevices().get(0).getPreKey().getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getDevices().get(0).getPreKey().getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getDevices().get(0).getSignedPreKey()).isEqualTo(existsAccount.getDevice(1).get().getSignedPreKey());

    verify(keys).get(eq(EXISTS_NUMBER), eq(1L));
    verifyNoMoreInteractions(keys);
  }


  @Test
  public void validMultiRequestTestV1() throws Exception {
    PreKeyResponseV1 results = resources.client().resource(String.format("/v1/keys/%s/*", EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(PreKeyResponseV1.class);

    assertThat(results.getKeys().size()).isEqualTo(3);

    PreKeyV1 result = results.getKeys().get(0);

    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID);

    result = results.getKeys().get(1);
    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY2.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY2.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID2);

    result = results.getKeys().get(2);
    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY4.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY4.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());
    assertThat(result.getRegistrationId()).isEqualTo(SAMPLE_REGISTRATION_ID4);

    verify(keys).get(eq(EXISTS_NUMBER));
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void validMultiRequestTestV2() throws Exception {
    PreKeyResponseV2 results = resources.client().resource(String.format("/v2/keys/%s/*", EXISTS_NUMBER))
                                        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                        .get(PreKeyResponseV2.class);

    assertThat(results.getDevices().size()).isEqualTo(3);
    assertThat(results.getIdentityKey()).isEqualTo(existsAccount.getIdentityKey());

    PreKeyV2 signedPreKey   = results.getDevices().get(0).getSignedPreKey();
    PreKeyV2 preKey         = results.getDevices().get(0).getPreKey();
    long     registrationId = results.getDevices().get(0).getRegistrationId();
    long     deviceId       = results.getDevices().get(0).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID);
    assertThat(signedPreKey.getKeyId()).isEqualTo(SAMPLE_SIGNED_KEY.getKeyId());
    assertThat(signedPreKey.getPublicKey()).isEqualTo(SAMPLE_SIGNED_KEY.getPublicKey());
    assertThat(deviceId).isEqualTo(1);

    signedPreKey   = results.getDevices().get(1).getSignedPreKey();
    preKey         = results.getDevices().get(1).getPreKey();
    registrationId = results.getDevices().get(1).getRegistrationId();
    deviceId       = results.getDevices().get(1).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY2.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY2.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID2);
    assertThat(signedPreKey.getKeyId()).isEqualTo(SAMPLE_SIGNED_KEY2.getKeyId());
    assertThat(signedPreKey.getPublicKey()).isEqualTo(SAMPLE_SIGNED_KEY2.getPublicKey());
    assertThat(deviceId).isEqualTo(2);

    signedPreKey   = results.getDevices().get(2).getSignedPreKey();
    preKey         = results.getDevices().get(2).getPreKey();
    registrationId = results.getDevices().get(2).getRegistrationId();
    deviceId       = results.getDevices().get(2).getDeviceId();

    assertThat(preKey.getKeyId()).isEqualTo(SAMPLE_KEY4.getKeyId());
    assertThat(preKey.getPublicKey()).isEqualTo(SAMPLE_KEY4.getPublicKey());
    assertThat(registrationId).isEqualTo(SAMPLE_REGISTRATION_ID4);
    assertThat(signedPreKey).isNull();
    assertThat(deviceId).isEqualTo(4);

    verify(keys).get(eq(EXISTS_NUMBER));
    verifyNoMoreInteractions(keys);
  }


  @Test
  public void invalidRequestTestV1() throws Exception {
    ClientResponse response = resources.client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  public void invalidRequestTestV2() throws Exception {
    ClientResponse response = resources.client().resource(String.format("/v2/keys/%s", NOT_EXISTS_NUMBER))
                                       .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                       .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  public void anotherInvalidRequestTestV2() throws Exception {
    ClientResponse response = resources.client().resource(String.format("/v2/keys/%s/22", EXISTS_NUMBER))
                                       .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                                       .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(404);
  }

  @Test
  public void unauthorizedRequestTestV1() throws Exception {
    ClientResponse response =
        resources.client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
            .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);

    response =
        resources.client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
  }

  @Test
  public void unauthorizedRequestTestV2() throws Exception {
    ClientResponse response =
        resources.client().resource(String.format("/v2/keys/%s/1", EXISTS_NUMBER))
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
                 .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);

    response =
        resources.client().resource(String.format("/v2/keys/%s/1", EXISTS_NUMBER))
                 .get(ClientResponse.class);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
  }

  @Test
  public void putKeysTestV1() throws Exception {
    final PreKeyV1 newKey        = new PreKeyV1(1L, 31337, "foobar", "foobarbaz");
    final PreKeyV1 lastResortKey = new PreKeyV1(1L, 0xFFFFFF, "fooz", "foobarbaz");

    List<PreKeyV1> preKeys = new LinkedList<PreKeyV1>() {{
      add(newKey);
    }};

    PreKeyStateV1 preKeyList = new PreKeyStateV1();
    preKeyList.setKeys(preKeys);
    preKeyList.setLastResortKey(lastResortKey);

    ClientResponse response =
        resources.client().resource("/v1/keys")
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class, preKeyList);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(204);

    ArgumentCaptor<List>     listCaptor       = ArgumentCaptor.forClass(List.class    );
    ArgumentCaptor<PreKeyV1> lastResortCaptor = ArgumentCaptor.forClass(PreKeyV1.class);
    verify(keys).store(eq(AuthHelper.VALID_NUMBER), eq(1L), listCaptor.capture(), lastResortCaptor.capture());

    List<PreKeyV1> capturedList = listCaptor.getValue();
    assertThat(capturedList.size() == 1);
    assertThat(capturedList.get(0).getIdentityKey().equals("foobarbaz"));
    assertThat(capturedList.get(0).getKeyId() == 31337);
    assertThat(capturedList.get(0).getPublicKey().equals("foobar"));

    assertThat(lastResortCaptor.getValue().getPublicKey().equals("fooz"));
    assertThat(lastResortCaptor.getValue().getIdentityKey().equals("foobarbaz"));

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq("foobarbaz"));
    verify(accounts).update(AuthHelper.VALID_ACCOUNT);
  }

  @Test
  public void putKeysTestV2() throws Exception {
    final PreKeyV2     preKey        = new PreKeyV2(31337, "foobar");
    final PreKeyV2     lastResortKey = new PreKeyV2(31339, "barbar");
    final SignedPreKey signedPreKey  = new SignedPreKey(31338, "foobaz", "myvalidsig");
    final String       identityKey   = "barbar";

    List<PreKeyV2> preKeys = new LinkedList<PreKeyV2>() {{
      add(preKey);
    }};

    PreKeyStateV2 preKeyState = new PreKeyStateV2(identityKey, signedPreKey, preKeys, lastResortKey);

    ClientResponse response =
        resources.client().resource("/v2/keys")
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .type(MediaType.APPLICATION_JSON_TYPE)
                 .put(ClientResponse.class, preKeyState);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(204);

    ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(keys).store(eq(AuthHelper.VALID_NUMBER), eq(1L), listCaptor.capture(), eq(lastResortKey));

    List<PreKeyV2> capturedList = listCaptor.getValue();
    assertThat(capturedList.size() == 1);
    assertThat(capturedList.get(0).getKeyId() == 31337);
    assertThat(capturedList.get(0).getPublicKey().equals("foobar"));

    verify(AuthHelper.VALID_ACCOUNT).setIdentityKey(eq("barbar"));
    verify(AuthHelper.VALID_DEVICE).setSignedPreKey(eq(signedPreKey));
    verify(accounts).update(AuthHelper.VALID_ACCOUNT);
  }


}