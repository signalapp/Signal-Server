package org.whispersystems.textsecuregcm.controllers;

import com.amazonaws.HttpMethod;
import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptor;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.federation.FederatedClient;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.UrlSigner;
import org.whispersystems.textsecuregcm.storage.Account;

import javax.ws.rs.WebApplicationException;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AttachmentControllerTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private RateLimiters rateLimiters;
    private FederatedClientManager federatedClientManager;
    private UrlSigner urlSigner;
    private AttachmentController attachmentController;
    private Account account;

    @Before
    public void setUp() throws Exception {
        rateLimiters           = mock(RateLimiters.class);
        federatedClientManager = mock(FederatedClientManager.class);
        urlSigner              = mock(UrlSigner.class);
        account                = mock(Account.class);
        attachmentController   = new AttachmentController(rateLimiters, federatedClientManager, urlSigner);
    }

    @Test
    public void allocateAttachment_shouldReturnAttachmentWithURLFromSigner() throws Exception {
        URL url = new URL("http://www.foo.com");
        when(urlSigner.getPreSignedUrl(anyLong(), eq(HttpMethod.PUT))).thenReturn(url);

        AttachmentDescriptor attachmentDescriptor = attachmentController.allocateAttachment(account);

        assertThat(attachmentDescriptor.getLocation()).isEqualTo("http://www.foo.com");
    }

    @Test(expected = RateLimitExceededException.class)
    public void allocateAttachment_shouldBubbleUpException_whenAccountIsLimitedAndValidationThrows() throws Exception {
        RateLimiter attachmentLimiter = mock(RateLimiter.class);
        when(rateLimiters.getAttachmentLimiter()).thenReturn(attachmentLimiter);
        doThrow(new RateLimitExceededException("Oh no!")).when(attachmentLimiter).validate("12345");
        when(account.isRateLimited()).thenReturn(true);
        when(account.getNumber()).thenReturn("12345");

        attachmentController.allocateAttachment(account);
    }

    @Test
    public void redirectToAttachment_shouldReturnPresignedUrl_whenRelayIsNotPresent() throws Exception {
        URL url = new URL("http://www.baz.com");
        when(urlSigner.getPreSignedUrl(67890, HttpMethod.GET)).thenReturn(url);

        AttachmentUri attachmentUri = attachmentController.redirectToAttachment(account, 67890, Optional.absent());

        assertThat(attachmentUri.getLocation().toString()).isEqualTo("http://www.baz.com");
    }

    @Test
    public void redirectToAttachment_shouldReturnSignedAttachment_whenRelayIsPresent() throws Exception {
        URL url = new URL("http://www.bar.com");
        FederatedClient federatedClient = mock(FederatedClient.class);
        when(federatedClientManager.getClient("aRelay")).thenReturn(federatedClient);
        when(federatedClient.getSignedAttachmentUri(1122)).thenReturn(url);

        AttachmentUri attachmentUri = attachmentController.redirectToAttachment(account, 1122, Optional.of("aRelay"));

        assertThat(attachmentUri.getLocation().toString()).isEqualTo("http://www.bar.com");
    }

    @Test
    public void redirectToAttachment_shouldReturn404_whenRelayIsPresentAndFederatedClientManagerThrowsNoSuchPeer() throws Exception {
        when(federatedClientManager.getClient("aRelay")).thenThrow(new NoSuchPeerException("No Such Peer!"));
        expectedEx.expect(WebApplicationException.class);
        expectedEx.expectMessage("HTTP 404 Not Found");

        attachmentController.redirectToAttachment(account, 1122, Optional.of("aRelay"));
    }
}
