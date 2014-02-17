/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;

import java.io.IOException;

/**
 */
public class DisabledGlobalOrdinals implements GlobalOrdinalsBuilder {

    @Override
    public IndexFieldData.WithOrdinals build(IndexReader indexReader, final IndexFieldData.WithOrdinals indexFieldData, Settings settings) throws IOException {
        return new IndexFieldData.WithOrdinals() {
            @Override
            public AtomicFieldData.WithOrdinals load(AtomicReaderContext context) {
                throw fail();
            }

            @Override
            public AtomicFieldData.WithOrdinals loadDirect(AtomicReaderContext context) throws Exception {
                throw fail();
            }

            @Override
            public WithOrdinals loadGlobal(IndexReader indexReader) {
                return this;
            }

            @Override
            public WithOrdinals localGlobalDirect(IndexReader indexReader) throws Exception {
                return this;
            }

            @Override
            public FieldMapper.Names getFieldNames() {
                return indexFieldData.getFieldNames();
            }

            @Override
            public boolean valuesOrdered() {
                return false;
            }

            @Override
            public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
                throw fail();
            }

            @Override
            public void clear() {
            }

            @Override
            public void clear(IndexReader reader) {
            }

            @Override
            public Index index() {
                return indexFieldData.index();
            }

            private ElasticsearchIllegalStateException fail() {
                return new ElasticsearchIllegalStateException("Global ordinals is forbidden on " + getFieldNames().name());
            }

        };
    }
}
