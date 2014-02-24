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

  public static MultiBasicAuthProvider<FederatedPeer, Account> getAuthenticator() {
    AccountsManager           accounts    = mock(AccountsManager.class          );
    Account                   account     = mock(Account.class                  );
    Device                    device      = mock(Device.class                   );
    AuthenticationCredentials credentials = mock(AuthenticationCredentials.class);

    when(credentials.verify("foo")).thenReturn(true);
    when(device.getAuthenticationCredentials()).thenReturn(credentials);
    when(device.getId()).thenReturn(1L);
    when(account.getDevice(anyLong())).thenReturn(Optional.of(device));
    when(account.getNumber()).thenReturn(VALID_NUMBER);
    when(account.getAuthenticatedDevice()).thenReturn(Optional.of(device));
    when(account.getRelay()).thenReturn(Optional.<String>absent());
    when(accounts.get(VALID_NUMBER)).thenReturn(Optional.of(account));

    List<FederatedPeer> peer = new LinkedList<FederatedPeer>() {{
      add(new FederatedPeer("cyanogen", "https://foo", "foofoo", "bazzzzz"));
    }};

    FederationConfiguration federationConfiguration = mock(FederationConfiguration.class);
    when(federationConfiguration.getPeers()).thenReturn(peer);

    return new MultiBasicAuthProvider<>(new FederatedPeerAuthenticator(federationConfiguration),
                                        FederatedPeer.class,
                                        new AccountAuthenticator(accounts),
                                        Account.class, "WhisperServer");
  }

  public static String getAuthHeader(String number, String password) {
    return "Basic " + Base64.encodeBytes((number + ":" + password).getBytes());
  }
}
