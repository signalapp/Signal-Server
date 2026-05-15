/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.jetty;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
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
  public boolean isContainedIn(final Resource r) throws MalformedURLException {
    return false;
  }

  @Override
  public void close() {

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
  public long lastModified() {
    return 0;
  }

  @Override
  public long length() {
    return 0;
  }

  @Override
  public URI getURI() {
    return null;
  }

  @Override
  public File getFile() throws IOException {
    return null;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream(data);
  }

  @Override
  public ReadableByteChannel getReadableByteChannel() throws IOException {
    return null;
  }

  @Override
  public boolean delete() throws SecurityException {
    return false;
  }

  @Override
  public boolean renameTo(final Resource dest) throws SecurityException {
    return false;
  }

  @Override
  public String[] list() {
    return new String[]{name};
  }

  @Override
  public Resource addPath(final String path) throws IOException, MalformedURLException {
    return this;
  }
}
