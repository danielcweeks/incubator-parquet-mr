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
package org.apache.parquet.column;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ParquetPropertiesTest {

  @Test
  public void testNonContiguousPageWriteDisabledByDefault() {
    ParquetProperties props = ParquetProperties.builder().build();
    assertFalse("Non-contiguous page writes must be disabled by default", props.isNonContiguousPageWriteEnabled());
  }

  @Test
  public void testNonContiguousPageWriteDefaultConstantIsFalse() {
    assertFalse(
        "DEFAULT_NON_CONTIGUOUS_PAGE_WRITE_ENABLED must be false",
        ParquetProperties.DEFAULT_NON_CONTIGUOUS_PAGE_WRITE_ENABLED);
  }

  @Test
  public void testNonContiguousPageWriteEnabledViaBuilder() {
    ParquetProperties props = ParquetProperties.builder()
        .withNonContiguousPageWriteEnabled(true)
        .build();
    assertTrue(
        "Builder must enable non-contiguous page writes when withNonContiguousPageWriteEnabled(true) is called",
        props.isNonContiguousPageWriteEnabled());
  }

  @Test
  public void testNonContiguousPageWriteCopyPreservesValue() {
    ParquetProperties original = ParquetProperties.builder()
        .withNonContiguousPageWriteEnabled(true)
        .build();
    ParquetProperties copy = ParquetProperties.copy(original).build();
    assertEquals(
        "ParquetProperties.copy must preserve nonContiguousPageWriteEnabled",
        original.isNonContiguousPageWriteEnabled(),
        copy.isNonContiguousPageWriteEnabled());
    assertTrue(
        "Copied properties must report non-contiguous writes enabled", copy.isNonContiguousPageWriteEnabled());
  }
}
