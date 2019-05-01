/**
 * Copyright (C) 2014 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.websocket.servlet;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class BufferingServletInputStream extends ServletInputStream {

  private final ByteArrayInputStream buffer;

  public BufferingServletInputStream(byte[] body) {
    this.buffer = new ByteArrayInputStream(body);
  }

  @Override
  public int read(byte[] buf, int offset, int length) {
    return buffer.read(buf, offset, length);
  }

  @Override
  public int read(byte[] buf) {
    return read(buf, 0, buf.length);
  }

  @Override
  public int read() throws IOException {
    return buffer.read();
  }

  @Override
  public int available() {
    return buffer.available();
  }

  @Override
  public boolean isFinished() {
    return available() > 0;
  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public void setReadListener(ReadListener readListener) {

  }
}
