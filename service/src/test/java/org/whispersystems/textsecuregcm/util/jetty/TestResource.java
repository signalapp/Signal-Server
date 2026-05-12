/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.jetty;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Base64;
import org.eclipse.jetty.util.resource.Resource;

public class TestResource extends Resource {

  private final String name;
  private final byte[] data;

  private TestResource(String name, byte[] data) {
    this.name = name;
    this.data = data;
  }

  public static Resource fromBase64Mime(String name, String base64) {
    return new TestResource(name, Base64.getMimeDecoder().decode(base64));
  }

  @Override
  public Path getPath() {
    return null;
  }

  @Override
  public InputStream newInputStream() {
    return new ByteArrayInputStream(data);
  }


  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  @Override
  public boolean isReadable() {
    return true;
  }

  @Override
  public long length() {
    return data.length;
  }

  @Override
  public URI getURI() {
    return null;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getFileName() {
    return "";
  }

  @Override
  public Resource resolve(final String subUriPath) {
    return null;
  }

}
