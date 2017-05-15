package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.ProfileController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ProfileControllerTest {

  @Test
  public void testProfileGet() throws RateLimitExceededException {
    Account         requestAccount  = mock(Account.class        );
    Account         profileAccount  = mock(Account.class        );
    RateLimiter     rateLimiter     = mock(RateLimiter.class    );
    RateLimiters    rateLimiters    = mock(RateLimiters.class   );
    AccountsManager accountsManager = mock(AccountsManager.class);

    when(rateLimiters.getProfileLimiter()).thenReturn(rateLimiter);
    when(requestAccount.getNumber()).thenReturn("foo");
    when(profileAccount.getIdentityKey()).thenReturn("bar");
    when(accountsManager.get(eq("baz"))).thenReturn(Optional.of(profileAccount));

    ProfileController profileController = new ProfileController(rateLimiters, accountsManager);
    Profile           result            = profileController.getProfile(requestAccount, "baz");

    assertEquals(result.getIdentityKey(), "bar");

    verify(accountsManager, times(1)).get(eq("baz"));
    verify(rateLimiters, times(1)).getProfileLimiter();
    verify(rateLimiter, times(1)).validate("foo");
  }


}
