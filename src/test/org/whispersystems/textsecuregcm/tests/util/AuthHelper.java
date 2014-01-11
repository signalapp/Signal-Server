package org.whispersystems.textsecuregcm.tests.util;

import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.auth.DeviceAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.FederatedPeerAuthenticator;
import org.whispersystems.textsecuregcm.auth.MultiBasicAuthProvider;
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Base64;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthHelper {
  public static final long DEFAULT_DEVICE_ID = 1;

  public static final String VALID_NUMBER   = "+14150000000";
  public static final String VALID_PASSWORD = "foo";

  public static final String INVVALID_NUMBER  = "+14151111111";
  public static final String INVALID_PASSWORD = "bar";

  public static MultiBasicAuthProvider<FederatedPeer, Device> getAuthenticator() {
    AccountsManager           accounts    = mock(AccountsManager.class);
    Device device = mock(Device.class);
    AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(credentials.verify("foo")).thenReturn(true);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(accounts.get(VALID_NUMBER, DEFAULT_DEVICE_ID)).thenReturn(Optional.of(device));

    return new MultiBasicAuthProvider<>(new FederatedPeerAuthenticator(new FederationConfiguration()),
                                        FederatedPeer.class,
                                        new DeviceAuthenticator(accounts),
                                        Device.class, "WhisperServer");
  }

  public static String getAuthHeader(String number, String password) {
    return "Basic " + Base64.encodeBytes((number + ":" + password).getBytes());
  }

  public static String getV2AuthHeader(String number, long deviceId, String password) {
    return "Basic " + Base64.encodeBytes((number + "." + deviceId + ":" + password).getBytes());
  }
}
