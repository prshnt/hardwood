/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that the `bloom_filter_offset` / `bloom_filter_length` fields
/// on Thrift `ColumnMetaData` (fields 14/15) are surfaced on the public
/// [dev.hardwood.metadata.ColumnMetaData] record. The fixture
/// `bloom_filter_test.parquet` has a bloom filter on column `id` and none
/// on column `value`, so a single footer parse exercises both the
/// populated and the absent shape.
class BloomFilterMetadataTest {

    @Test
    void surfacesBloomFilterOffsetAndLengthFromFooter() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/bloom_filter_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);

            ColumnChunk idChunk = rowGroup.columns().get(0);
            assertThat(idChunk.metaData().pathInSchema().toString()).isEqualTo("id");
            assertThat(idChunk.metaData().bloomFilterOffset()).isNotNull().isPositive();
            assertThat(idChunk.metaData().bloomFilterLength()).isNotNull().isPositive();

            ColumnChunk valueChunk = rowGroup.columns().get(1);
            assertThat(valueChunk.metaData().pathInSchema().toString()).isEqualTo("value");
            assertThat(valueChunk.metaData().bloomFilterOffset()).isNull();
            assertThat(valueChunk.metaData().bloomFilterLength()).isNull();
        }
    }
}
