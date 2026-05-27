/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.junit.Test;

public class TestParquetWriterBuilder {

  /**
   * Minimal concrete subclass of the abstract {@link ParquetWriter.Builder}, used only
   * to exercise builder methods. {@code getWriteSupport} returns {@code null} because
   * no test in this class actually calls {@code build()}.
   */
  private static final class TestBuilder extends ParquetWriter.Builder<Object, TestBuilder> {
    TestBuilder() {
      super();
    }

    @Override
    protected TestBuilder self() {
      return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    protected WriteSupport<Object> getWriteSupport(Configuration conf) {
      return null;
    }
  }

  private static ParquetProperties buildEncodingProperties(ParquetWriter.Builder<?, ?> b) throws Exception {
    Field f = ParquetWriter.Builder.class.getDeclaredField("encodingPropsBuilder");
    f.setAccessible(true);
    return ((ParquetProperties.Builder) f.get(b)).build();
  }

  @Test
  public void testNonContiguousPageWriteDisabledByDefault() throws Exception {
    TestBuilder builder = new TestBuilder();
    assertFalse(
        "ParquetWriter.Builder must default non-contiguous page writes to disabled",
        buildEncodingProperties(builder).isNonContiguousPageWriteEnabled());
  }

  @Test
  public void testWithNonContiguousPageWriteEnabledTrue() throws Exception {
    TestBuilder builder = new TestBuilder();
    TestBuilder returned = builder.withNonContiguousPageWriteEnabled(true);
    assertSame("withNonContiguousPageWriteEnabled must return the same builder for chaining", builder, returned);
    assertTrue(
        "withNonContiguousPageWriteEnabled(true) must enable the flag on the underlying ParquetProperties",
        buildEncodingProperties(builder).isNonContiguousPageWriteEnabled());
  }

  @Test
  public void testWithNonContiguousPageWriteEnabledFalseOverridesPriorTrue() throws Exception {
    TestBuilder builder = new TestBuilder();
    builder.withNonContiguousPageWriteEnabled(true);
    builder.withNonContiguousPageWriteEnabled(false);
    assertFalse(
        "withNonContiguousPageWriteEnabled(false) must override a prior true value",
        buildEncodingProperties(builder).isNonContiguousPageWriteEnabled());
  }
}
