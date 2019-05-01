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
import java.io.IOException;

public class NullServletOutputStream extends ServletOutputStream {
  @Override
  public void write(int b) throws IOException {}

  @Override
  public void write(byte[] buf) {}

  @Override
  public void write(byte[] buf, int offset, int len) {}

  @Override
  public boolean isReady() {
    return false;
  }

  @Override
  public void setWriteListener(WriteListener writeListener) {

  }
}
