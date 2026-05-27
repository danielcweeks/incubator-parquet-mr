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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the non-contiguous page write path enabled via
 * {@code ParquetWriter.Builder#withNonContiguousPageWriteEnabled(true)}.
 *
 * <p>The current Java reader does not yet understand {@code data_page_offset == -1}, so these
 * tests verify the writer's output by inspecting the file's footer and on-disk page bytes
 * directly. A future reader-side change will enable full round-trip reads.
 */
public class TestNonContiguousPageWrite {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final MessageType MULTI_COLUMN_SCHEMA = parseMessageType(
      "message test { "
          + "required int64 long_field; "
          + "required binary binary_field; "
          + "required int32 int_field; "
          + "}");

  @Test
  public void columnChunksRecordSentinelOffsets() throws Exception {
    Configuration conf = new Configuration();
    org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(MULTI_COLUMN_SCHEMA, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(MULTI_COLUMN_SCHEMA);

    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());

    int recordCount = 2_000;
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageSize(1024)
        .withRowGroupSize(64 * 1024L)
        .withDictionaryEncoding(false)
        .withNonContiguousPageWriteEnabled(true)
        .build()) {
      for (int i = 0; i < recordCount; i++) {
        writer.write(f.newGroup()
            .append("long_field", (long) i)
            .append("binary_field", "value_" + i)
            .append("int_field", i));
      }
    }

    ParquetMetadata footer = readFooter(conf, path, NO_FILTER);
    assertTrue("expected at least one block", footer.getBlocks().size() >= 1);
    for (BlockMetaData block : footer.getBlocks()) {
      assertTrue("expected at least one column", block.getColumns().size() == 3);
      for (ColumnChunkMetaData column : block.getColumns()) {
        assertEquals(
            "non-contiguous mode must write data_page_offset = -1 for " + column.getPath(),
            -1L,
            column.getFirstDataPageOffset());
        assertEquals(
            "getStartingPos() must return -1 when pages are non-contiguous",
            -1L,
            column.getStartingPos());
      }
    }

    // Inspect the raw on-disk format to confirm ColumnChunk.file_offset is -1 on every column
    // and that RowGroup.file_offset is unset.
    FileMetaData fileMetaData = readRawFileMetaData(conf, path);
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
      assertTrue(
          "RowGroup.file_offset must be unset for non-contiguous row groups",
          !rowGroup.isSetFile_offset());
      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        assertEquals("ColumnChunk.file_offset must be -1", -1L, columnChunk.getFile_offset());
        assertEquals(
            "ColumnMetaData.data_page_offset must be -1",
            -1L,
            columnChunk.getMeta_data().getData_page_offset());
      }
    }
  }

  @Test
  public void offsetIndexEntriesAreAbsoluteAndPagesInterleave() throws Exception {
    // Schema uses two binary columns whose values do not compress, so both produce many
    // mid-row-group pages — exercising the interleaved-write path on a single row group.
    MessageType schema = parseMessageType(
        "message test { required binary a; required binary b; }");
    Configuration conf = new Configuration();
    org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());

    int recordCount = 5_000;
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageSize(1024)
        .withRowGroupSize(1024L * 1024L)
        .withDictionaryEncoding(false)
        .withNonContiguousPageWriteEnabled(true)
        .build()) {
      for (int i = 0; i < recordCount; i++) {
        writer.write(f.newGroup()
            .append("a", "alpha_" + i + "_padding_" + i + "_" + i + "_" + i)
            .append("b", "beta_" + i + "_payload_" + i + "_" + i + "_" + i + "_x"));
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      ParquetMetadata footer = reader.getFooter();
      BlockMetaData block = footer.getBlocks().get(0);
      Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
      long fileLength = HadoopInputFile.fromPath(path, conf).getLength();
      for (ColumnChunkMetaData column : block.getColumns()) {
        OffsetIndex oi = reader.readOffsetIndex(column);
        assertNotNull("OffsetIndex must be present for " + column.getPath(), oi);
        assertTrue("expected at least one page for " + column.getPath(), oi.getPageCount() > 0);
        for (int i = 0; i < oi.getPageCount(); i++) {
          long offset = oi.getOffset(i);
          assertTrue("page offset must be positive: " + offset, offset > 0);
          assertTrue(
              "page offset+size must lie within the file: " + offset + " + " + oi.getCompressedPageSize(i),
              offset + oi.getCompressedPageSize(i) <= fileLength);
        }
        offsetIndexes.put(column.getPath(), oi);
      }

      // Verify pages from different columns interleave in the file. We check that at least
      // one column's pages do not form a contiguous run in the file — i.e., some other
      // column's page falls in the middle of its OffsetIndex offsets.
      assertTrue(
          "expected pages from at least one column to be split by another column's pages",
          pagesAreInterleaved(offsetIndexes));

      // Spot check: the bytes at each recorded page offset should be a valid PageHeader.
      try (SeekableInputStream in =
          HadoopInputFile.fromPath(path, conf).newStream()) {
        for (OffsetIndex oi : offsetIndexes.values()) {
          for (int i = 0; i < oi.getPageCount(); i++) {
            in.seek(oi.getOffset(i));
            assertNotNull(
                "PageHeader should be deserializable at offset " + oi.getOffset(i),
                Util.readPageHeader(in));
          }
        }
      }
    }
  }

  @Test
  public void dictionaryPageLengthIsRecorded() throws Exception {
    Configuration conf = new Configuration();
    MessageType dictSchema = parseMessageType("message test { required binary dict_field; }");
    org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(dictSchema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(dictSchema);

    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageSize(1024)
        .withRowGroupSize(64 * 1024L)
        .withDictionaryEncoding(true)
        .withNonContiguousPageWriteEnabled(true)
        .build()) {
      for (int i = 0; i < 2_000; i++) {
        writer.write(f.newGroup().append("dict_field", "value_" + (i % 8)));
      }
    }

    FileMetaData fileMetaData = readRawFileMetaData(conf, path);
    boolean foundDictionaryPageLength = false;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        org.apache.parquet.format.ColumnMetaData md = columnChunk.getMeta_data();
        if (md.isSetDictionary_page_offset()) {
          assertTrue(
              "dictionary_page_length must be set whenever a dictionary page is present "
                  + "and non-contiguous writes are enabled",
              md.isSetDictionary_page_length());
          assertTrue(
              "dictionary_page_length must be positive: " + md.getDictionary_page_length(),
              md.getDictionary_page_length() > 0);
          foundDictionaryPageLength = true;
        }
      }
    }
    assertTrue(
        "expected at least one column chunk with a recorded dictionary_page_length",
        foundDictionaryPageLength);

    // The read-side ColumnChunkMetaData must also surface the new field.
    ParquetMetadata footer = readFooter(conf, path, NO_FILTER);
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.hasDictionaryPage()) {
          assertTrue(
              "ColumnChunkMetaData.getDictionaryPageLength() should be > 0 for dictionary columns",
              column.getDictionaryPageLength() > 0);
        }
      }
    }
  }

  @Test
  public void encryptionWithNonContiguousIsRejected() throws Exception {
    Configuration conf = new Configuration();
    org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(MULTI_COLUMN_SCHEMA, conf);

    byte[] footerKey = "0123456789012345".getBytes();
    byte[] columnKey = "1234567890123450".getBytes();
    ColumnEncryptionProperties columnProps = ColumnEncryptionProperties.builder("binary_field")
        .withKey(columnKey)
        .withKeyID("kc")
        .build();
    Map<ColumnPath, ColumnEncryptionProperties> columnMap = new HashMap<>();
    columnMap.put(columnProps.getPath(), columnProps);
    FileEncryptionProperties encryption = FileEncryptionProperties.builder(footerKey)
        .withFooterKeyID("kf")
        .withEncryptedColumns(columnMap)
        .build();

    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());

    SimpleGroupFactory factory = new SimpleGroupFactory(MULTI_COLUMN_SCHEMA);
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> {
          try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
              .withConf(conf)
              .withEncryption(encryption)
              .withNonContiguousPageWriteEnabled(true)
              .build()) {
            writer.write(factory.newGroup()
                .append("long_field", 1L)
                .append("binary_field", "x")
                .append("int_field", 1));
          }
        });
    assertTrue(
        "expected UnsupportedOperationException to mention non-contiguous + encryption: " + ex.getMessage(),
        ex.getMessage().toLowerCase().contains("non-contiguous"));
  }

  /**
   * Returns true when at least one column has another column's page landing between two of
   * its own pages (in file-offset order). That's sufficient evidence the writer is producing
   * non-contiguous page layouts; if all columns' pages formed contiguous runs we'd see a
   * single column transition per pair of consecutive offsets.
   */
  private static boolean pagesAreInterleaved(Map<ColumnPath, OffsetIndex> offsetIndexes) {
    // Build a flat list of (offset, columnPath) sorted by offset.
    java.util.List<long[]> entries = new java.util.ArrayList<>();
    java.util.Map<Long, ColumnPath> ownerByOffset = new java.util.HashMap<>();
    for (Map.Entry<ColumnPath, OffsetIndex> e : offsetIndexes.entrySet()) {
      OffsetIndex oi = e.getValue();
      for (int i = 0; i < oi.getPageCount(); i++) {
        long offset = oi.getOffset(i);
        entries.add(new long[] {offset, 0});
        ownerByOffset.put(offset, e.getKey());
      }
    }
    entries.sort((a, b) -> Long.compare(a[0], b[0]));
    // For each column, check that the indices at which its pages appear are not all
    // consecutive in the sorted-by-offset order.
    for (ColumnPath column : offsetIndexes.keySet()) {
      java.util.List<Integer> indices = new java.util.ArrayList<>();
      for (int i = 0; i < entries.size(); i++) {
        if (column.equals(ownerByOffset.get(entries.get(i)[0]))) {
          indices.add(i);
        }
      }
      if (indices.size() < 2) {
        continue;
      }
      for (int i = 1; i < indices.size(); i++) {
        if (indices.get(i) - indices.get(i - 1) > 1) {
          return true;
        }
      }
    }
    return false;
  }

  private static FileMetaData readRawFileMetaData(Configuration conf, Path path) throws Exception {
    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
    long len = inputFile.getLength();
    try (SeekableInputStream in = inputFile.newStream()) {
      // Footer layout: ... | FileMetaData | i32 footerLength | "PAR1"
      in.seek(len - 8);
      byte[] footerLenBuf = new byte[4];
      in.readFully(footerLenBuf);
      int footerLen = ((footerLenBuf[0] & 0xff))
          | ((footerLenBuf[1] & 0xff) << 8)
          | ((footerLenBuf[2] & 0xff) << 16)
          | ((footerLenBuf[3] & 0xff) << 24);
      byte[] magic = new byte[4];
      in.readFully(magic);
      assertEquals("PAR1 magic", "PAR1", new String(magic));
      long footerStart = len - 8L - footerLen;
      in.seek(footerStart);
      byte[] footerBytes = new byte[footerLen];
      in.readFully(footerBytes);
      return Util.readFileMetaData(new java.io.ByteArrayInputStream(footerBytes));
    }
  }

  // Reference a couple of imports that some compilers might prune.
  @SuppressWarnings("unused")
  private void silence() {
    assertNull(null);
  }
}
