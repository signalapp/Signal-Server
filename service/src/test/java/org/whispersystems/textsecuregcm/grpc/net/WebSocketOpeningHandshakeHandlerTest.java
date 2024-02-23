package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WebSocketOpeningHandshakeHandlerTest extends AbstractLeakDetectionTest {

  private EmbeddedChannel embeddedChannel;

  private static final String AUTHENTICATED_PATH = "/authenticated";
  private static final String ANONYMOUS_PATH = "/anonymous";

  @BeforeEach
  void setUp() {
    embeddedChannel = new EmbeddedChannel(new WebSocketOpeningHandshakeHandler(AUTHENTICATED_PATH, ANONYMOUS_PATH));
  }

  @ParameterizedTest
  @ValueSource(strings = { AUTHENTICATED_PATH, ANONYMOUS_PATH })
  void handleValidRequest(final String path) {
    final FullHttpRequest request = buildRequest(HttpMethod.GET, path,
        new DefaultHttpHeaders().add(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET));

    try {
      embeddedChannel.writeOneInbound(request);

      assertEquals(1, request.refCnt());
      assertEquals(1, embeddedChannel.inboundMessages().size());
      assertEquals(request, embeddedChannel.inboundMessages().poll());
    } finally {
      request.release();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { AUTHENTICATED_PATH, ANONYMOUS_PATH })
  void handleUpgradeRequired(final String path) {
    final FullHttpRequest request = buildRequest(HttpMethod.GET, path, new DefaultHttpHeaders());

    embeddedChannel.writeOneInbound(request);

    assertEquals(0, request.refCnt());
    assertHttpResponse(HttpResponseStatus.UPGRADE_REQUIRED);
  }

  @Test
  void handleBadPath() {
    final FullHttpRequest request = buildRequest(HttpMethod.GET, "/incorrect",
        new DefaultHttpHeaders().add(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET));

    embeddedChannel.writeOneInbound(request);

    assertEquals(0, request.refCnt());
    assertHttpResponse(HttpResponseStatus.NOT_FOUND);
  }

  @ParameterizedTest
  @ValueSource(strings = { AUTHENTICATED_PATH, ANONYMOUS_PATH })
  void handleMethodNotAllowed(final String path) {
    final FullHttpRequest request = buildRequest(HttpMethod.DELETE, path,
        new DefaultHttpHeaders().add(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET));

    embeddedChannel.writeOneInbound(request);

    assertEquals(0, request.refCnt());
    assertHttpResponse(HttpResponseStatus.METHOD_NOT_ALLOWED);
  }

  private void assertHttpResponse(final HttpResponseStatus expectedStatus) {
    assertEquals(1, embeddedChannel.outboundMessages().size());

    final FullHttpResponse response = assertInstanceOf(FullHttpResponse.class, embeddedChannel.outboundMessages().poll());

    //noinspection DataFlowIssue
    assertEquals(expectedStatus, response.status());
  }

  private FullHttpRequest buildRequest(final HttpMethod method, final String path, final HttpHeaders headers) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
        method,
        path,
        Unpooled.buffer(0),
        headers,
        new DefaultHttpHeaders(true));
  }
}
