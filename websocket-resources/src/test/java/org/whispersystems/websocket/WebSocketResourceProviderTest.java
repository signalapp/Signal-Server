package org.whispersystems.websocket;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.jetty.websocket.api.CloseStatus;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.SubProtocol;
import org.whispersystems.websocket.session.WebSocketSession;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class WebSocketResourceProviderTest {

  @Test
  public void testOnConnect() {
    ApplicationHandler                       applicationHandler = mock(ApplicationHandler.class      );
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class     );
    WebSocketConnectListener                 connectListener    = mock(WebSocketConnectListener.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1",
                                                                                                  applicationHandler, requestLog,
                                                                                                  new TestPrincipal("fooz"),
                                                                                                  new ProtobufWebSocketMessageFactory(),
                                                                                                  Optional.of(connectListener),
                                                                                                  30000                                 );

    Session        session = mock(Session.class       );
    UpgradeRequest request = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);

    provider.onWebSocketConnect(session);

    verify(session, never()).close(anyInt(), anyString());
    verify(session, never()).close();
    verify(session, never()).close(any(CloseStatus.class));

    ArgumentCaptor<WebSocketSessionContext> contextArgumentCaptor = ArgumentCaptor.forClass(WebSocketSessionContext.class);
    verify(connectListener).onWebSocketConnect(contextArgumentCaptor.capture());

    assertThat(contextArgumentCaptor.getValue().getAuthenticated(TestPrincipal.class).getName()).isEqualTo("fooz");
  }

  @Test
  public void testMockedRouteMessageSuccess() throws Exception {
    ApplicationHandler                       applicationHandler = mock(ApplicationHandler.class );
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    ContainerResponse response = mock(ContainerResponse.class);
    when(response.getStatus()).thenReturn(200);
    when(response.getStatusInfo()).thenReturn(new Response.StatusType() {
      @Override
      public int getStatusCode() {
        return 200;
      }

      @Override
      public Response.Status.Family getFamily() {
        return Response.Status.Family.SUCCESSFUL;
      }

      @Override
      public String getReasonPhrase() {
        return "OK";
      }
    });

    ArgumentCaptor<OutputStream> responseOutputStream = ArgumentCaptor.forClass(OutputStream.class);

    when(applicationHandler.apply(any(ContainerRequest.class), responseOutputStream.capture()))
        .thenAnswer((Answer<CompletableFuture<ContainerResponse>>) invocation -> {
          responseOutputStream.getValue().write("hello world!".getBytes());
          return CompletableFuture.completedFuture(response);
        });

    provider.onWebSocketConnect(session);

    verify(session, never()).close(anyInt(), anyString());
    verify(session, never()).close();
    verify(session, never()).close(any(CloseStatus.class));

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/bar", new LinkedList<>(), Optional.of("hello world!".getBytes())).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ContainerRequest> requestCaptor  = ArgumentCaptor.forClass(ContainerRequest.class);
    ArgumentCaptor<ByteBuffer>       responseCaptor = ArgumentCaptor.forClass(ByteBuffer.class      );

    verify(applicationHandler).apply(requestCaptor.capture(), any(OutputStream.class));

    ContainerRequest bundledRequest = requestCaptor.getValue();

    assertThat(bundledRequest.getRequest().getMethod()).isEqualTo("GET");
    assertThat(bundledRequest.getBaseUri().toString()).isEqualTo("/");
    assertThat(bundledRequest.getPath(false)).isEqualTo("bar");

    verify(requestLog).log(eq("127.0.0.1"), eq(bundledRequest), eq(response));
    verify(remoteEndpoint).sendBytesByFuture(responseCaptor.capture());

    SubProtocol.WebSocketMessage responseMessageContainer = SubProtocol.WebSocketMessage.parseFrom(responseCaptor.getValue().array());
    assertThat(responseMessageContainer.getResponse().getId()).isEqualTo(111L);
    assertThat(responseMessageContainer.getResponse().getStatus()).isEqualTo(200);
    assertThat(responseMessageContainer.getResponse().getMessage()).isEqualTo("OK");
    assertThat(responseMessageContainer.getResponse().getBody()).isEqualTo(ByteString.copyFrom("hello world!".getBytes()));
  }

  @Test
  public void testMockedRouteMessageFailure() throws Exception {
    ApplicationHandler                       applicationHandler = mock(ApplicationHandler.class );
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    when(applicationHandler.apply(any(ContainerRequest.class), any(OutputStream.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("foo")));

    provider.onWebSocketConnect(session);

    verify(session, never()).close(anyInt(), anyString());
    verify(session, never()).close();
    verify(session, never()).close(any(CloseStatus.class));

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/bar", new LinkedList<>(), Optional.of("hello world!".getBytes())).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ContainerRequest> requestCaptor = ArgumentCaptor.forClass(ContainerRequest.class);

    verify(applicationHandler).apply(requestCaptor.capture(), any(OutputStream.class));

    ContainerRequest bundledRequest = requestCaptor.getValue();

    assertThat(bundledRequest.getRequest().getMethod()).isEqualTo("GET");
    assertThat(bundledRequest.getBaseUri().toString()).isEqualTo("/");
    assertThat(bundledRequest.getPath(false)).isEqualTo("bar");

    ArgumentCaptor<ByteBuffer> responseCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseCaptor.capture());

    SubProtocol.WebSocketMessage responseMessageContainer = SubProtocol.WebSocketMessage.parseFrom(responseCaptor.getValue().array());
    assertThat(responseMessageContainer.getResponse().getStatus()).isEqualTo(500);
    assertThat(responseMessageContainer.getResponse().getMessage()).isEqualTo("Error response");
    assertThat(responseMessageContainer.getResponse().hasBody()).isFalse();
  }

  @Test
  public void testActualRouteMessageSuccess() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/hello", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getMessage()).isEqualTo("OK");
    assertThat(response.getBody()).isEqualTo(ByteString.copyFrom("Hello!".getBytes()));
  }

  @Test
  public void testActualRouteMessageNotFound() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));


    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/doesntexist", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(404);
    assertThat(response.getMessage()).isEqualTo("Not Found");
    assertThat(response.hasBody()).isFalse();
  }

  @Test
  public void testActualRouteMessageAuthorized() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("authorizedUserName"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/world", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getMessage()).isEqualTo("OK");
    assertThat(response.getBody().toStringUtf8()).isEqualTo("World: authorizedUserName");
  }

  @Test
  public void testActualRouteMessageUnauthorized() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, null, new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/world", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(401);
    assertThat(response.hasBody()).isFalse();
  }

  @Test
  public void testActualRouteMessageOptionalAuthorizedPresent() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("something"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/optional", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getMessage()).isEqualTo("OK");
    assertThat(response.getBody().toStringUtf8()).isEqualTo("World: something");
  }

  @Test
  public void testActualRouteMessageOptionalAuthorizedEmpty() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, null, new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/optional", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getMessage()).isEqualTo("OK");
    assertThat(response.getBody().toStringUtf8()).isEqualTo("Empty world");
  }

  @Test
  public void testActualRouteMessagePutAuthenticatedEntity() throws InvalidProtocolBufferException, JsonProcessingException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("gooduser"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "PUT", "/v1/test/some/testparam", List.of("Content-Type: application/json"), Optional.of(new ObjectMapper().writeValueAsBytes(new TestResource.TestEntity("mykey", 1001)))).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getMessage()).isEqualTo("OK");
    assertThat(response.getBody().toStringUtf8()).isEqualTo("gooduser:testparam:mykey:1001");
  }

  @Test
  public void testActualRouteMessagePutAuthenticatedBadEntity() throws InvalidProtocolBufferException, JsonProcessingException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("gooduser"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "PUT", "/v1/test/some/testparam", List.of("Content-Type: application/json"), Optional.of(new ObjectMapper().writeValueAsBytes(new TestResource.TestEntity("mykey", 5)))).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getMessage()).isEqualTo("Bad Request");
    assertThat(response.hasBody()).isFalse();
  }

  @Test
  public void testActualRouteMessageExceptionMapping() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new TestExceptionMapper());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("gooduser"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/exception/map", List.of("Content-Type: application/json"), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(1337);
    assertThat(response.hasBody()).isFalse();
  }

  @Test
  public void testActualRouteSessionContextInjection() throws InvalidProtocolBufferException {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(new TestResource());
    resourceConfig.register(new TestExceptionMapper());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("gooduser"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    Session        session        = mock(Session.class       );
    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request        = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/keepalive", new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    ArgumentCaptor<ByteBuffer> requestCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytes(requestCaptor.capture(), any(WriteCallback.class));

    SubProtocol.WebSocketRequestMessage requestMessage = getRequest(requestCaptor);
    assertThat(requestMessage.getVerb()).isEqualTo("GET");
    assertThat(requestMessage.getPath()).isEqualTo("/v1/miccheck");
    assertThat(requestMessage.getBody().toStringUtf8()).isEqualTo("smert ze smert");

    byte[] clientResponse  = new ProtobufWebSocketMessageFactory().createResponse(requestMessage.getId(), 200, "OK", new LinkedList<>(), Optional.of("my response".getBytes())).toByteArray();

    provider.onWebSocketBinary(clientResponse, 0, clientResponse.length);

    ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);

    verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

    SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

    assertThat(response.getId()).isEqualTo(111L);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getMessage()).isEqualTo("OK");
    assertThat(response.getBody().toStringUtf8()).isEqualTo("my response");
  }

  private SubProtocol.WebSocketResponseMessage getResponse(ArgumentCaptor<ByteBuffer> responseCaptor) throws InvalidProtocolBufferException {
    return SubProtocol.WebSocketMessage.parseFrom(responseCaptor.getValue().array()).getResponse();
  }

  private SubProtocol.WebSocketRequestMessage getRequest(ArgumentCaptor<ByteBuffer> requestCaptor) throws InvalidProtocolBufferException {
    return SubProtocol.WebSocketMessage.parseFrom(requestCaptor.getValue().array()).getRequest();
  }


  public static class TestPrincipal implements Principal {

    private final String name;

    private TestPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  public static class TestException extends Exception {
    public TestException(String message) {
      super(message);
    }
  }

  @Provider
  public static class TestExceptionMapper implements ExceptionMapper<TestException> {

    @Override
    public Response toResponse(TestException exception) {
      return Response.status(1337).build();
    }
  }

  @Path("/v1/test")
  public static class TestResource {

    @GET
    @Path("/hello")
    public String testGetHello() {
      return "Hello!";
    }

    @GET
    @Path("/world")
    public String testAuthorizedHello(@Auth TestPrincipal user) {
      if (user == null) throw new AssertionError();

      return "World: " + user.getName();
    }

    @GET
    @Path("/optional")
    public String testAuthorizedHello(@Auth Optional<TestPrincipal> user) {
      if (user.isPresent()) return "World: " + user.get().getName();
      else                  return "Empty world";
    }

    @PUT
    @Path("/some/{param}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response testSet(@Auth TestPrincipal user, @PathParam ("param") String param, @Valid TestEntity entity) {
      return Response.ok(user.name + ":" + param + ":" + entity.key + ":" + entity.value).build();
    }

    @GET
    @Path("/exception/map")
    public Response testExceptionMapping() throws TestException {
      throw new TestException("I'd like to map this");
    }

    @GET
    @Path("/keepalive")
    public CompletableFuture<Response> testContextInjection(@WebSocketSession WebSocketSessionContext context) {
      if (context == null) {
        throw new AssertionError();
      }

      return context.getClient()
                    .sendRequest("GET", "/v1/miccheck", new LinkedList<>(), Optional.of("smert ze smert".getBytes()))
                    .thenApply(response -> Response.ok().entity(new String(response.getBody().get())).build());
    }

    public static class TestEntity {

      public TestEntity(String key, long value) {
        this.key   = key;
        this.value = value;
      }

      public TestEntity() {
      }

      @JsonProperty
      @NotEmpty
      private String key;

      @JsonProperty
      @Min(100)
      private long value;

    }
  }

}
