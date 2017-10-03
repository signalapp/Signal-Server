package org.whispersystems.textsecuregcm.tests.util;

import com.google.common.base.Optional;
import org.whispersystems.dropwizard.simpleauth.AuthDynamicFeature;
import org.whispersystems.dropwizard.simpleauth.BasicCredentialAuthFilter;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.FederatedPeerAuthenticator;
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Base64;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthHelper {
  public static final String VALID_NUMBER   = "+14150000000";
  public static final String VALID_PASSWORD = "foo";

  public static final String VALID_NUMBER_TWO = "+201511111110";
  public static final String VALID_PASSWORD_TWO = "baz";

  public static final String INVVALID_NUMBER  = "+14151111111";
  public static final String INVALID_PASSWORD = "bar";

  public static AccountsManager           ACCOUNTS_MANAGER  = mock(AccountsManager.class          );
  public static Account                   VALID_ACCOUNT     = mock(Account.class                  );
  public static Account                   VALID_ACCOUNT_TWO = mock(Account.class);
  public static Device                    VALID_DEVICE      = mock(Device.class                   );
  public static Device                    VALID_DEVICE_TWO  = mock(Device.class);
  private static AuthenticationCredentials VALID_CREDENTIALS = mock(AuthenticationCredentials.class);
  private static AuthenticationCredentials VALID_CREDENTIALS_TWO = mock(AuthenticationCredentials.class);

  public static AuthDynamicFeature getAuthFilter() {
    when(VALID_CREDENTIALS.verify("foo")).thenReturn(true);
    when(VALID_CREDENTIALS_TWO.verify("baz")).thenReturn(true);
    when(VALID_DEVICE.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS);
    when(VALID_DEVICE_TWO.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS_TWO);
    when(VALID_DEVICE.getId()).thenReturn(1L);
    when(VALID_DEVICE_TWO.getId()).thenReturn(1L);
    when(VALID_ACCOUNT.getDevice(anyLong())).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT_TWO.getDevice(eq(1L))).thenReturn(Optional.of(VALID_DEVICE_TWO));
    when(VALID_ACCOUNT_TWO.getActiveDeviceCount()).thenReturn(6);
    when(VALID_ACCOUNT.getNumber()).thenReturn(VALID_NUMBER);
    when(VALID_ACCOUNT_TWO.getNumber()).thenReturn(VALID_NUMBER_TWO);
    when(VALID_ACCOUNT.getAuthenticatedDevice()).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT_TWO.getAuthenticatedDevice()).thenReturn(Optional.of(VALID_DEVICE_TWO));
    when(VALID_ACCOUNT.getRelay()).thenReturn(Optional.<String>absent());
    when(VALID_ACCOUNT_TWO.getRelay()).thenReturn(Optional.<String>absent());
    when(ACCOUNTS_MANAGER.get(VALID_NUMBER)).thenReturn(Optional.of(VALID_ACCOUNT));
    when(ACCOUNTS_MANAGER.get(VALID_NUMBER_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));

    List<FederatedPeer> peer = new LinkedList<FederatedPeer>() {{
      add(new FederatedPeer("cyanogen", "https://foo", "foofoo", "bazzzzz"));
    }};

    FederationConfiguration federationConfiguration = mock(FederationConfiguration.class);
    when(federationConfiguration.getPeers()).thenReturn(peer);

    return new AuthDynamicFeature(new BasicCredentialAuthFilter.Builder<Account>()
                                      .setAuthenticator(new AccountAuthenticator(ACCOUNTS_MANAGER))
                                      .setPrincipal(Account.class)
                                      .buildAuthFilter(),
                                  new BasicCredentialAuthFilter.Builder<FederatedPeer>()
                                      .setAuthenticator(new FederatedPeerAuthenticator(federationConfiguration))
                                      .setPrincipal(FederatedPeer.class)
                                      .buildAuthFilter());
  }

  public static String getAuthHeader(String number, String password) {
    return "Basic " + Base64.encodeBytes((number + ":" + password).getBytes());
  }
}
