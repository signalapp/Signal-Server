/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import com.webauthn4j.anchor.TrustAnchorRepository;
import com.webauthn4j.data.attestation.authenticator.AAGUID;
import com.webauthn4j.util.CertificateUtil;
import com.webauthn4j.verifier.attestation.trustworthiness.certpath.DefaultCertPathTrustworthinessVerifier;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Set;

/**
 * A {@link com.webauthn4j.verifier.attestation.trustworthiness.certpath.CertPathTrustworthinessVerifier} for validating
 * x5 certificate chains, pinned with apple's well known static device check root certificate.
 */
public class AppleDeviceCheckTrustAnchor extends DefaultCertPathTrustworthinessVerifier {

  // The location of a PEM encoded certificate for Apple's DeviceCheck root certificate
  // https://www.apple.com/certificateauthority/Apple_App_Attestation_Root_CA.pem
  private static String APPLE_DEVICE_CHECK_ROOT_CERT_RESOURCE_NAME = "apple_device_check.pem";

  public AppleDeviceCheckTrustAnchor() {
    super(new StaticTrustAnchorRepository(loadDeviceCheckRootCert()));
  }

  private record StaticTrustAnchorRepository(X509Certificate rootCert) implements TrustAnchorRepository {

    @Override
    public Set<TrustAnchor> find(final AAGUID aaguid) {
      return Collections.singleton(new TrustAnchor(rootCert, null));
    }

    @Override
    public Set<TrustAnchor> find(final byte[] attestationCertificateKeyIdentifier) {
      return Collections.singleton(new TrustAnchor(rootCert, null));
    }
  }

  private static X509Certificate loadDeviceCheckRootCert() {
    try (InputStream stream = AppleDeviceCheckTrustAnchor.class.getResourceAsStream(
        APPLE_DEVICE_CHECK_ROOT_CERT_RESOURCE_NAME)) {
      if (stream == null) {
        throw new IllegalArgumentException("Resource not found: " + APPLE_DEVICE_CHECK_ROOT_CERT_RESOURCE_NAME);
      }
      return CertificateUtil.generateX509Certificate(stream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
