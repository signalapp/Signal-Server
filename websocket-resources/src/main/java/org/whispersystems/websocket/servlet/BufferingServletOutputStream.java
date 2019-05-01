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

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BufferingServletOutputStream extends ServletOutputStream {

  private final ByteArrayOutputStream buffer;

  public BufferingServletOutputStream(ByteArrayOutputStream buffer) {
    this.buffer = buffer;
  }

  @Override
  public void write(byte[] buf, int offset, int length) {
    buffer.write(buf, offset, length);
  }

  @Override
  public void write(byte[] buf) {
    write(buf, 0, buf.length);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.write(b);
  }

  @Override
  public void flush() {

  }

  @Override
  public void close() {

  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public void setWriteListener(WriteListener writeListener) {

  }
}
