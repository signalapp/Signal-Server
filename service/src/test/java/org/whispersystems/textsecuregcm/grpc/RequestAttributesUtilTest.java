package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.GetRequestAttributesResponse;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

class RequestAttributesUtilTest {

  private static DefaultEventLoopGroup eventLoopGroup;

  private GrpcClientConnectionManager grpcClientConnectionManager;

  private Server server;
  private ManagedChannel managedChannel;

  @BeforeAll
  static void setUpBeforeAll() {
    eventLoopGroup = new DefaultEventLoopGroup();
  }

  @BeforeEach
  void setUp() throws IOException {
    final LocalAddress serverAddress = new LocalAddress("test-request-metadata-server");

    grpcClientConnectionManager = mock(GrpcClientConnectionManager.class);

    when(grpcClientConnectionManager.getRemoteAddress(any()))
        .thenReturn(Optional.of(InetAddresses.forString("127.0.0.1")));

    // `RequestAttributesInterceptor` operates on `LocalAddresses`, so we need to do some slightly fancy plumbing to make
    // sure that we're using local channels and addresses
    server = NettyServerBuilder.forAddress(serverAddress)
        .channelType(LocalServerChannel.class)
        .bossEventLoopGroup(eventLoopGroup)
        .workerEventLoopGroup(eventLoopGroup)
        .intercept(new RequestAttributesInterceptor(grpcClientConnectionManager))
        .addService(new RequestAttributesServiceImpl())
        .build()
        .start();

    managedChannel = NettyChannelBuilder.forAddress(serverAddress)
        .channelType(LocalChannel.class)
        .eventLoopGroup(eventLoopGroup)
        .usePlaintext()
        .build();
  }

  @AfterEach
  void tearDown() {
    managedChannel.shutdown();
    server.shutdown();
  }

  @AfterAll
  static void tearDownAfterAll() throws InterruptedException {
    eventLoopGroup.shutdownGracefully().await();
  }

  @Test
  void getAcceptableLanguages() {
    when(grpcClientConnectionManager.getAcceptableLanguages(any()))
        .thenReturn(Optional.empty());

    assertTrue(getRequestAttributes().getAcceptableLanguagesList().isEmpty());

    when(grpcClientConnectionManager.getAcceptableLanguages(any()))
        .thenReturn(Optional.of(Locale.LanguageRange.parse("en,ja")));

    assertEquals(List.of("en", "ja"), getRequestAttributes().getAcceptableLanguagesList());
  }

  @Test
  void getAvailableAcceptedLocales() {
    when(grpcClientConnectionManager.getAcceptableLanguages(any()))
        .thenReturn(Optional.empty());

    assertTrue(getRequestAttributes().getAvailableAcceptedLocalesList().isEmpty());

    when(grpcClientConnectionManager.getAcceptableLanguages(any()))
        .thenReturn(Optional.of(Locale.LanguageRange.parse("en,ja")));

    final GetRequestAttributesResponse response = getRequestAttributes();

    assertFalse(response.getAvailableAcceptedLocalesList().isEmpty());
    response.getAvailableAcceptedLocalesList().forEach(languageTag -> {
      final Locale locale = Locale.forLanguageTag(languageTag);
      assertTrue("en".equals(locale.getLanguage()) || "ja".equals(locale.getLanguage()));
    });
  }

  @Test
  void getRemoteAddress() {
    when(grpcClientConnectionManager.getRemoteAddress(any()))
        .thenReturn(Optional.empty());

    GrpcTestUtils.assertStatusException(Status.INTERNAL, this::getRequestAttributes);

    final String remoteAddressString = "6.7.8.9";

    when(grpcClientConnectionManager.getRemoteAddress(any()))
        .thenReturn(Optional.of(InetAddresses.forString(remoteAddressString)));

    assertEquals(remoteAddressString, getRequestAttributes().getRemoteAddress());
  }

  @Test
  void getUserAgent() throws UnrecognizedUserAgentException {
    when(grpcClientConnectionManager.getUserAgent(any()))
        .thenReturn(Optional.empty());

    assertFalse(getRequestAttributes().hasUserAgent());

    final UserAgent userAgent = UserAgentUtil.parseUserAgentString("Signal-Desktop/1.2.3 Linux");

    when(grpcClientConnectionManager.getUserAgent(any()))
        .thenReturn(Optional.of(userAgent));

    final GetRequestAttributesResponse response = getRequestAttributes();
    assertTrue(response.hasUserAgent());
    assertEquals("DESKTOP", response.getUserAgent().getPlatform());
    assertEquals("1.2.3", response.getUserAgent().getVersion());
    assertEquals("Linux", response.getUserAgent().getAdditionalSpecifiers());
  }

  private GetRequestAttributesResponse getRequestAttributes() {
    return RequestAttributesGrpc.newBlockingStub(managedChannel)
        .getRequestAttributes(GetRequestAttributesRequest.newBuilder().build());
  }
}
