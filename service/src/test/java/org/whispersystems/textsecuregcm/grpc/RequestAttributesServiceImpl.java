package org.whispersystems.textsecuregcm.grpc;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.signal.chat.rpc.GetAuthenticatedDeviceRequest;
import org.signal.chat.rpc.GetAuthenticatedDeviceResponse;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.GetRequestAttributesResponse;
import org.signal.chat.rpc.RequestAttributesGrpc;
import org.signal.chat.rpc.UserAgent;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class RequestAttributesServiceImpl extends RequestAttributesGrpc.RequestAttributesImplBase {

  @Override
  public void getRequestAttributes(final GetRequestAttributesRequest request,
      final StreamObserver<GetRequestAttributesResponse> responseObserver) {

    final GetRequestAttributesResponse.Builder responseBuilder = GetRequestAttributesResponse.newBuilder();

    RequestAttributesUtil.getAcceptableLanguages().ifPresent(acceptableLanguages ->
        acceptableLanguages.forEach(languageRange -> responseBuilder.addAcceptableLanguages(languageRange.toString())));

    RequestAttributesUtil.getAvailableAcceptedLocales().forEach(locale ->
        responseBuilder.addAvailableAcceptedLocales(locale.toLanguageTag()));

    responseBuilder.setRemoteAddress(RequestAttributesUtil.getRemoteAddress().getHostAddress());

    RequestAttributesUtil.getUserAgent().ifPresent(userAgent -> responseBuilder.setUserAgent(UserAgent.newBuilder()
            .setPlatform(userAgent.platform().toString())
            .setVersion(userAgent.version().toString())
            .setAdditionalSpecifiers(StringUtils.stripToEmpty(userAgent.additionalSpecifiers()))
        .build()));

    RequestAttributesUtil.getRawUserAgent().ifPresent(responseBuilder::setRawUserAgent);

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getAuthenticatedDevice(final GetAuthenticatedDeviceRequest request,
      final StreamObserver<GetAuthenticatedDeviceResponse> responseObserver) {

    final GetAuthenticatedDeviceResponse.Builder responseBuilder = GetAuthenticatedDeviceResponse.newBuilder();

    try {
      final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

      responseBuilder.setAccountIdentifier(UUIDUtil.toByteString(authenticatedDevice.accountIdentifier()));
      responseBuilder.setDeviceId(authenticatedDevice.deviceId());
    } catch (final Exception ignored) {
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }
}
