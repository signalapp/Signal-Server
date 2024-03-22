/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static java.util.Objects.requireNonNull;

import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.stub.AbstractBlockingStub;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.MockitoAnnotations;
import org.whispersystems.textsecuregcm.auth.grpc.MockAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.storage.Device;

/**
 * Base class for the common case of gRPC services tests. This base class makes some assumptions
 * and introduces some constraints on the implementing classes with a goal of simplifying the process
 * of creating a test for the most of the gRPC services.
 * <ul>
 * <li>
 *   Test classes extending this class will have to override the {@link #createServiceBeforeEachTest()} method
 *   with the logic that creates an instance of the service to test. This method is called before each test and should
 *   contain other setup code that would normally go into {@code @BeforeEach} method.
 * </li>
 * <li>
 *   This base class takes care of creating two service stubs: {@code authenticatedServiceStub} and {@code unauthenticatedServiceStub}.
 *   Normally, those stubs are created by the call to the {@code newBlockingStub()} method on the {@code *Stub} class, e.g.:
 *   <pre>CallingGrpc.newBlockingStub(GRPC_SERVER_EXTENSION_AUTHENTICATED.getChannel());</pre>
 *   In this class, those stubs are created by the {@link #createStub(Channel)} method that has a default implementation that is based on
 *   figuring out the name of the {@code `*Grpc`} class and invoking {@code `*Grpc.newBlockingStub()`} method with reflection.
 * </li>
 * <li>
 *   This class takes care of initializing {@code Mockito} annotations processing, so implementing classes
 *   can annotate their fields with {@code @Mock} and have those mocks ready by the time {@link #createServiceBeforeEachTest()} is called.
 * </li>
 * </ul>
 * @param <SERVICE> Class of the gRPC service that is being tested.
 * @param <STUB> Class of the gRPC service stub.
 */
public abstract class SimpleBaseGrpcTest<SERVICE extends BindableService, STUB extends AbstractBlockingStub<STUB>> {

  @RegisterExtension
  protected static final GrpcServerExtension GRPC_SERVER_EXTENSION_AUTHENTICATED = new GrpcServerExtension();

  @RegisterExtension
  protected static final GrpcServerExtension GRPC_SERVER_EXTENSION_UNAUTHENTICATED = new GrpcServerExtension();

  protected static final UUID AUTHENTICATED_ACI = UUID.randomUUID();

  protected static final byte AUTHENTICATED_DEVICE_ID = Device.PRIMARY_ID;

  private AutoCloseable mocksCloseable;

  private final MockAuthenticationInterceptor mockAuthenticationInterceptor = new MockAuthenticationInterceptor();
  private final MockRequestAttributesInterceptor mockRequestAttributesInterceptor = new MockRequestAttributesInterceptor();

  private SERVICE service;

  private STUB authenticatedServiceStub;

  private STUB unauthenticatedServiceStub;


  /**
   * This method is invoked before each test and is expected to create an instance of the gRPC service
   * that is being tested and also to perform all necessary before-each setup.
   * </p>
   * Extending classes may have their own {@code @BeforeEach} method, but it will be called after this method.
   * @return an instance of the gRPC service.
   */
  protected abstract SERVICE createServiceBeforeEachTest();

  /**
   * The default implementation of this method is based on figuring out the name of the {@code `*Grpc`} class
   * and invoking {@code `*Grpc.newBlockingStub()`} method with reflection.
   * <p>
   * Overriding this method can be helpful if addutional configuration of the stub is required, e.g. adding interceptors:
   * <pre>
   * protected ProfileAnonymousGrpc.ProfileAnonymousBlockingStub createStub(final Channel channel) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
   *   final Metadata metadata = new Metadata();
   *   metadata.put(AcceptLanguageInterceptor.ACCEPTABLE_LANGUAGES_GRPC_HEADER, "en-us");
   *   metadata.put(UserAgentInterceptor.USER_AGENT_GRPC_HEADER, "Signal-Android/1.2.3");
   *   return super.createStub(channel)
   *       .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
   * }
   * </pre>
   * @param channel grpc channel to create create the stub for.
   * @return and instance of the service stub.
   */
  protected STUB createStub(final Channel channel) throws
      ClassNotFoundException,
      NoSuchMethodException,
      InvocationTargetException,
      IllegalAccessException {
    final String serviceClassName = service.bindService().getServiceDescriptor().getName();
    final String grpcClassName = serviceClassName + "Grpc";
    final Class<?> grpcClass = ClassLoader.getSystemClassLoader().loadClass(grpcClassName);
    final Method newBlockingStubMethod = grpcClass.getMethod("newBlockingStub", Channel.class);
    final Object stub = newBlockingStubMethod.invoke(null, channel);
    //noinspection unchecked
    return (STUB) stub;
  }

  @BeforeEach
  protected void baseSetup() {
    mocksCloseable = MockitoAnnotations.openMocks(this);
    service = requireNonNull(createServiceBeforeEachTest(), "created service must not be `null`");
    GrpcTestUtils.setupAuthenticatedExtension(
        GRPC_SERVER_EXTENSION_AUTHENTICATED, mockAuthenticationInterceptor, mockRequestAttributesInterceptor, AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID, service);
    GrpcTestUtils.setupUnauthenticatedExtension(GRPC_SERVER_EXTENSION_UNAUTHENTICATED, mockRequestAttributesInterceptor, service);
    try {
      authenticatedServiceStub = createStub(GRPC_SERVER_EXTENSION_AUTHENTICATED.getChannel());
      unauthenticatedServiceStub = createStub(GRPC_SERVER_EXTENSION_UNAUTHENTICATED.getChannel());
    } catch (Exception e) {
      throw new RuntimeException("Could not create a stub based on the service name. Try overriding `createStub()` method.");
    }
  }

  @AfterEach
  public void releaseMocks() throws Exception {
    mocksCloseable.close();
  }

  public MockAuthenticationInterceptor mockAuthenticationInterceptor() {
    return mockAuthenticationInterceptor;
  }

  protected SERVICE service() {
    return service;
  }

  protected STUB authenticatedServiceStub() {
    return authenticatedServiceStub;
  }

  protected STUB unauthenticatedServiceStub() {
    return unauthenticatedServiceStub;
  }

  protected MockRequestAttributesInterceptor getMockRequestAttributesInterceptor() {
    return mockRequestAttributesInterceptor;
  }

  protected MockAuthenticationInterceptor getMockAuthenticationInterceptor() {
    return mockAuthenticationInterceptor;
  }
}
