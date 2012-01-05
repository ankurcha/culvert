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

import java.util.Collection;

/**
 * Utility class for interacting with {@link Exception Exceptions}
 */
public final class Exceptions {

  /** to prevent instantiation */
  private Exceptions() {
  }

  /**
   * Wrap the throwable in a RuntimeException, if it isn't one already. If it
   * is, the original exception is returned.
   * @param t to check/wrap.
   * @return a RuntimeException (which can be thrown from any method)
   */
  public static RuntimeException asRuntime(Throwable t) {
    if (t instanceof RuntimeException)
      return (RuntimeException) t;
    return new RuntimeException(t);
  }

  /**
   * Represents multiple exceptions being thrown.
   */
  public static class MultiRuntimeException extends RuntimeException {
    private MultiRuntimeException(String message, Throwable firstSource) {
      super(message, firstSource);
    }
  }

  public static MultiRuntimeException MultiRuntimeException(
      Collection<Throwable> sources) {
    if (sources == null || sources.size() == 0) {
      return new MultiRuntimeException(
          "MultiRuntimeException thrown with no sources!",
          new RuntimeException());
    }
    StringBuilder msgBuilder = new StringBuilder();
    msgBuilder
        .append("Multiple remote exceptions thrown. Stack trace to first included, rest in message below.\n");
    int traceNum = 1;
    for (Throwable t : sources) {
      if (traceNum > 1) {
        msgBuilder.append(String.format("Exception %d\n", traceNum));
        msgBuilder.append(t.getClass().getName());
        msgBuilder.append("\n");
        msgBuilder.append(t.getMessage());
        msgBuilder.append("\n");
        for (StackTraceElement element : t.getStackTrace()) {
          msgBuilder.append(String.format("\t%s:%d %s\n",
              element.getClassName(), element.getLineNumber(),
              element.getMethodName()));
        }
      }
      traceNum++;
    }
    return new MultiRuntimeException(msgBuilder.toString(), sources.iterator()
        .next());
  }

}
