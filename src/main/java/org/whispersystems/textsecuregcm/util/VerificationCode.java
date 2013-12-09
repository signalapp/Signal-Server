/**
 * Copyright (C) 2013 Open WhisperSystems
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
package org.whispersystems.textsecuregcm.util;

public class VerificationCode {

  private String verificationCode;
  private String verificationCodeDisplay;
  private String verificationCodeSpeech;

  public VerificationCode(int verificationCode) {
    this.verificationCode        = verificationCode + "";
    this.verificationCodeDisplay = this.verificationCode.substring(0, 3) + "-" +
                                   this.verificationCode.substring(3, 6);
    this.verificationCodeSpeech  = delimit(verificationCode + "");
  }

  public String getVerificationCode() {
    return verificationCode;
  }

  public String getVerificationCodeDisplay() {
    return verificationCodeDisplay;
  }

  public String getVerificationCodeSpeech() {
    return verificationCodeSpeech;
  }

  private String delimit(String code) {
    String delimited = "";

    for (int i=0;i<code.length();i++) {
      delimited += code.charAt(i);

      if (i != code.length() - 1)
        delimited += ',';
    }

    return delimited;
  }

}
