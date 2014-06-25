package org.whispersystems.textsecuregcm.tests.util;

import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.FederatedPeerAuthenticator;
import org.whispersystems.textsecuregcm.auth.MultiBasicAuthProvider;
import org.whispersystems.textsecuregcm.configuration.FederationConfiguration;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Base64;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthHelper {
  public static final String VALID_NUMBER   = "+14150000000";
  public static final String VALID_PASSWORD = "foo";

  public static final String INVVALID_NUMBER  = "+14151111111";
  public static final String INVALID_PASSWORD = "bar";

  public static AccountsManager           ACCOUNTS_MANAGER  = mock(AccountsManager.class          );
  public static Account                   VALID_ACCOUNT     = mock(Account.class                  );
  public static Device                    VALID_DEVICE      = mock(Device.class                   );
  public static AuthenticationCredentials VALID_CREDENTIALS = mock(AuthenticationCredentials.class);

  public static MultiBasicAuthProvider<FederatedPeer, Account> getAuthenticator() {
    when(VALID_CREDENTIALS.verify("foo")).thenReturn(true);
    when(VALID_DEVICE.getAuthenticationCredentials()).thenReturn(VALID_CREDENTIALS);
    when(VALID_DEVICE.getId()).thenReturn(1L);
    when(VALID_ACCOUNT.getDevice(anyLong())).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT.getNumber()).thenReturn(VALID_NUMBER);
    when(VALID_ACCOUNT.getAuthenticatedDevice()).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT.getRelay()).thenReturn(Optional.<String>absent());
    when(ACCOUNTS_MANAGER.get(VALID_NUMBER)).thenReturn(Optional.of(VALID_ACCOUNT));

    List<FederatedPeer> peer = new LinkedList<FederatedPeer>() {{
      add(new FederatedPeer("cyanogen", "https://foo", "foofoo", "bazzzzz"));
    }};

    FederationConfiguration federationConfiguration = mock(FederationConfiguration.class);
    when(federationConfiguration.getPeers()).thenReturn(peer);

    return new MultiBasicAuthProvider<>(new FederatedPeerAuthenticator(federationConfiguration),
                                        FederatedPeer.class,
                                        new AccountAuthenticator(ACCOUNTS_MANAGER),
                                        Account.class, "WhisperServer");
  }

  public static String getAuthHeader(String number, String password) {
    return "Basic " + Base64.encodeBytes((number + ":" + password).getBytes());
  }
}
