/**
 * Copyright 2011 Booz Allen Hamilton.
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. Booz Allen Hamilton
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bah.culvert.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Map.Entry;

import javax.security.auth.login.ConfigurationSpi;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.index.Index;

public class ConfUtils {

  /**
   * Pack a configuration into a namespace in another one.
   * 
   * @param prefix The prefix to pack into. A '.' character is appended to the
   *        prefix.
   * @param toPack The configuration to pack.
   * @param toPackInto The configuration to pack <tt>toPack</tt> into.
   * @return <tt>toPackInto</tt>, with <tt>toPack</tt> packed in the prefix.
   */
  public static Configuration packConfigurationInPrefix(String prefix,
      Configuration toPack, Configuration toPackInto) {
    for (Entry<String, String> entry : toPack) {
      toPackInto.set(prefix + "." + entry.getKey(), entry.getValue());
    }
    return toPackInto;
  }

  /**
   * Unpack a configuration that has been prefixed. A '.' character must be used
   * to separate the prefixed keys from the prefix.
   * 
   * @param prefix The prefix to unpack.
   * @param toUnpackFrom The configuration to unpack from.
   * @return The unpacked configuration.
   */
  public static Configuration unpackConfigurationInPrefix(String prefix,
      Configuration toUnpackFrom) {
    Configuration conf = new Configuration(false);
    conf.clear();
    prefix += ".";
    for (Entry<String, String> entry : toUnpackFrom) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        key = key.substring(prefix.length());
        conf.set(key, entry.getValue());
      }
    }
    return conf;
  }

  /**
   * Combine a set of configurations into a single {@link Configuration}.
   * @param primary {@link Configuration} whose values take precedence over all
   *        others
   * @param secondary {@link Configuration Configurations} to add to the primary
   *        configuration. If the primary configuration has the key all ready,
   *        the value will not be updated. Precedence is given to the order of
   *        {@link Configuration Configurations}.
   * @return a new configuration that is a combination of the sent
   *         configurations. The original configurations are <i>not</i> modified
   */
  public static Configuration combineConfigurations(Configuration primary,
      Configuration... secondary) {
    Configuration out = new Configuration(primary);
    for (Configuration conf : secondary) {
      for (Entry<String, String> entry : conf) {
        if (out.get(entry.getKey()) == null)
          out.set(entry.getKey(), entry.getValue());
      }
    }
    return out;
  }

  /**
   * Get the contents of a key that might be binary.
   * @param isBinarySettingKey Tells us weather or not the field is binary.
   * @param potentiallyBinaryEncodedSetting The actual field name that might
   *        contain binary data.
   * @param conf The configuration to retrieve from
   * @return The decoded value to return.
   */
  public static byte[] getBinaryConfSetting(String isBinarySettingKey,
      String potentiallyBinaryEncodedSetting, Configuration conf) {
    String value = conf.get(potentiallyBinaryEncodedSetting);
    boolean isBase64 = conf.getBoolean(isBinarySettingKey, false);
    if (isBase64) {
      return Base64.decodeBase64(value.getBytes());
    } else {
      return value.getBytes();
    }
  }

  /**
   * Used to set a key indicating if the string value held by another
   * configuration key is a base64 encoded binary or not.
   * @param isValueBinaryEncodedSetting The key telling weather or not the other
   *        key (setting) is base64.
   * @param potentiallyEncodedSetting The actual key that might be base64
   *        encoded.
   * @param data The data to set as base64.
   * @param conf The configuration to do the setting on.
   */
  public static void setBinaryConfSetting(String isValueBinaryEncodedSetting,
      String potentiallyEncodedSetting, byte[] data, Configuration conf) {
    CharsetDecoder decoder = Index.UTF_8.newDecoder();
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    try {
      CharBuffer colFamString = decoder.decode(ByteBuffer.wrap(data));
      conf.setBoolean(isValueBinaryEncodedSetting, false);
      conf.set(potentiallyEncodedSetting, colFamString.toString());
    } catch (CharacterCodingException e) {
      conf.setBoolean(isValueBinaryEncodedSetting, true);
      conf.set(potentiallyEncodedSetting, new String(Base64.encodeBase64(data),
          Index.UTF_8));
    }
  }

}
