package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AcceptLanguageInterceptorTest {
  @ParameterizedTest
  @MethodSource
  void parseLocale(final String header, final List<Locale> expectedLocales) throws IOException, InterruptedException {
    final AtomicReference<List<Locale>> observedLocales = new AtomicReference<>(null);
    final EchoServiceImpl serviceImpl = new EchoServiceImpl() {
      @Override
      public void echo(EchoRequest req, StreamObserver<EchoResponse> responseObserver) {
        observedLocales.set(AcceptLanguageUtil.localeFromGrpcContext());
        super.echo(req, responseObserver);
      }
    };

    final Server testServer = InProcessServerBuilder.forName("AcceptLanguageTest")
        .directExecutor()
        .addService(serviceImpl)
        .intercept(new AcceptLanguageInterceptor())
        .intercept(new UserAgentInterceptor())
        .build()
        .start();

    try {
      final ManagedChannel channel = InProcessChannelBuilder.forName("AcceptLanguageTest")
          .directExecutor()
          .userAgent("Signal-Android/1.2.3")
          .build();

      final Metadata metadata = new Metadata();
      metadata.put(AcceptLanguageInterceptor.ACCEPTABLE_LANGUAGES_GRPC_HEADER, header);

      final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

      final EchoRequest request = EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("test request")).build();
      client.echo(request);
      assertEquals(expectedLocales, observedLocales.get());
    } finally {
      testServer.shutdownNow();
      testServer.awaitTermination();
    }
  }

  private static Stream<Arguments> parseLocale() {
    return Stream.of(
        // en-US-POSIX is a special locale that exists alongside en-US. It matches because of the definition of
        // basic filtering in RFC 4647 (https://datatracker.ietf.org/doc/html/rfc4647#section-3.3.1)
        Arguments.of("en-US,fr-CA", List.of(Locale.forLanguageTag("en-US-POSIX"), Locale.forLanguageTag("en-US"), Locale.forLanguageTag("fr-CA"))),
        Arguments.of("en-US; q=0.9, fr-CA", List.of(Locale.forLanguageTag("fr-CA"), Locale.forLanguageTag("en-US-POSIX"), Locale.forLanguageTag("en-US"))),
        Arguments.of("invalid-locale,fr-CA", List.of(Locale.forLanguageTag("fr-CA"))),
        Arguments.of("", Collections.emptyList()),
        Arguments.of("acompletely,unexpectedfor , mat", Collections.emptyList())
    );
  }
}
