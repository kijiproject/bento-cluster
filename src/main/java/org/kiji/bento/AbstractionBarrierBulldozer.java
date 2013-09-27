/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.bento;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Contains methods that use reflection to get and set private members of objects.
 */
final class AbstractionBarrierBulldozer {
  /**
   * Prevents instantiation of this utility class.
   */
  private AbstractionBarrierBulldozer() { }

  /**
   * Uses reflection to get the value of a field in an instance of a class. Any exceptions thrown
   * during this process are converted to {@link RuntimeException}.
   *
   * @param clazz The class of the instance whose field will be retrieved.
   * @param instance The class instance.
   * @param fieldName The name of the field whose value will be retrieved.
   * @param <T> The type of value retrieved.
   * @param <C> The type of the class of the instance whose field will be retrieved.
   * @return the value retrieved from the field.
   */
  @SuppressWarnings("unchecked")
  public static <T, C> T getField(Class<C> clazz, Object instance, String fieldName) {
    try {
      final Field field = clazz.getDeclaredField(fieldName);
      AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          field.setAccessible(true);
          return null;
        }
      });
      return (T) field.get(instance);
    } catch (Exception e) {
      throw new RuntimeException("There was a problem using reflection while configuring the "
          + "mini cluster.", e);
    }
  }

  /**
   * Uses reflection to set a field in an instance of a class. Any exceptions thrown during this
   * process are converted to {@link RuntimeException}.
   *
   * @param clazz The class of the instance whose field will be set.
   * @param instance The class instance.
   * @param fieldName The name of the field to set.
   * @param value The value for the field.
   * @param <T> The type of the class whose field will be set.
   */
  public static <T> void setField(Class<T> clazz, Object instance, String fieldName,
      Object value) {
    try {
      final Field field = clazz.getDeclaredField(fieldName);
      AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          field.setAccessible(true);
          return null;
        }
      });
      field.set(instance, value);
    } catch (Exception e) {
      throw new RuntimeException("There was a problem using reflection while configuring the "
          + "mini cluster.", e);
    }
  }
}
