package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netty.channel.local.LocalAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class RequestAttributesInterceptor implements ServerInterceptor {

  private final GrpcClientConnectionManager grpcClientConnectionManager;

  private static final Logger log = LoggerFactory.getLogger(RequestAttributesInterceptor.class);

  public RequestAttributesInterceptor(final GrpcClientConnectionManager grpcClientConnectionManager) {
    this.grpcClientConnectionManager = grpcClientConnectionManager;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    if (call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR) instanceof LocalAddress localAddress) {
      Context context = Context.current();

      {
        final Optional<InetAddress> maybeRemoteAddress = grpcClientConnectionManager.getRemoteAddress(localAddress);

        if (maybeRemoteAddress.isEmpty()) {
          // We should never have a call from a party whose remote address we can't identify
          log.warn("No remote address available");

          call.close(Status.INTERNAL, new Metadata());
          return new ServerCall.Listener<>() {};
        }

        context = context.withValue(RequestAttributesUtil.REMOTE_ADDRESS_CONTEXT_KEY, maybeRemoteAddress.get());
      }

      {
        final Optional<List<Locale.LanguageRange>> maybeAcceptLanguage =
            grpcClientConnectionManager.getAcceptableLanguages(localAddress);

        if (maybeAcceptLanguage.isPresent()) {
          context = context.withValue(RequestAttributesUtil.ACCEPT_LANGUAGE_CONTEXT_KEY, maybeAcceptLanguage.get());
        }
      }

      {
        final Optional<UserAgent> maybeUserAgent = grpcClientConnectionManager.getUserAgent(localAddress);

        if (maybeUserAgent.isPresent()) {
          context = context.withValue(RequestAttributesUtil.USER_AGENT_CONTEXT_KEY, maybeUserAgent.get());
        }
      }

      return Contexts.interceptCall(context, call, headers, next);
    } else {
      throw new AssertionError("Unexpected channel type: " + call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    }
  }
}
