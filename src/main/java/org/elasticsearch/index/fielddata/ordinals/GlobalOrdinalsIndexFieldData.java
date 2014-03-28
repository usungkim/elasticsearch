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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;

/**
 * {@link IndexFieldData} impl based on global ordinals.
 */
public final class GlobalOrdinalsIndexFieldData extends AbstractIndexComponent implements IndexFieldData.WithOrdinals, RamUsage {

    private final FieldMapper.Names fieldNames;
    private final Atomic[] atomicReaders;
    private final long memorySizeInBytes;
    private final long maxGlobalOrdinal;

    public GlobalOrdinalsIndexFieldData(Index index, Settings settings, FieldMapper.Names fieldNames, AtomicFieldData.WithOrdinals[] segmentAfd, LongValues globalOrdToFirstSegment, LongValues globalOrdToFirstSegmentOrd, LongValues[] segmentOrdToGlobalOrds, long memorySizeInBytes, long higestGlobalOrdinal) {
        super(index, settings);
        this.fieldNames = fieldNames;
        this.maxGlobalOrdinal = higestGlobalOrdinal + 1;
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(segmentAfd[i], globalOrdToFirstSegment, globalOrdToFirstSegmentOrd, segmentOrdToGlobalOrds[i], maxGlobalOrdinal);
        }
        this.memorySizeInBytes = memorySizeInBytes;
    }

    @Override
    public AtomicFieldData.WithOrdinals load(AtomicReaderContext context) {
        return atomicReaders[context.ord];
    }

    @Override
    public AtomicFieldData.WithOrdinals loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
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
        return fieldNames;
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void clear(IndexReader reader) {

    }

    @Override
    public long getMemorySizeInBytes() {
        return memorySizeInBytes;
    }

    private final class Atomic implements AtomicFieldData.WithOrdinals {

        private final long maxOrd;
        private final AtomicFieldData.WithOrdinals afd;
        private final LongValues segmentOrdToGlobalOrdLookup;
        private final LongValues globalOrdToFirstSegment;
        private final LongValues globalOrdToFirstSegmentOrd;

        private Atomic(WithOrdinals afd, LongValues globalOrdToFirstSegment, LongValues globalOrdToFirstSegmentOrd, LongValues segmentOrdToGlobalOrdLookup, long maxOrd) {
            this.afd = afd;
            this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            this.globalOrdToFirstSegment = globalOrdToFirstSegment;
            this.globalOrdToFirstSegmentOrd = globalOrdToFirstSegmentOrd;
            this.maxOrd = maxOrd;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
            BytesValues.WithOrdinals values = afd.getBytesValues(false);
            Ordinals.Docs actual = values.ordinals();
            Ordinals.Docs wrapper = new GlobalOrdinalsDocs(actual, segmentOrdToGlobalOrdLookup, memorySizeInBytes, maxOrd);

            return new BytesValues.WithOrdinals(wrapper) {

                int readerIndex;
                final IntObjectOpenHashMap<BytesValues.WithOrdinals> bytesValuesCache = new IntObjectOpenHashMap<>();

                @Override
                public BytesRef getValueByOrd(long globalOrd) {
                    final long segmentOrd = globalOrdToFirstSegmentOrd.get(globalOrd);
                    readerIndex = (int) globalOrdToFirstSegment.get(globalOrd);
                    if (bytesValuesCache.containsKey(readerIndex)) {
                        return bytesValuesCache.lget().getValueByOrd(segmentOrd);
                    } else {
                        BytesValues.WithOrdinals k = atomicReaders[readerIndex].afd.getBytesValues(false);
                        bytesValuesCache.put(readerIndex, k);
                        return k.getValueByOrd(segmentOrd);
                    }
                }

                @Override
                public BytesRef copyShared() {
                    return bytesValuesCache.get(readerIndex).copyShared();
                }
            };
        }

        @Override
        public boolean isMultiValued() {
            return afd.isMultiValued();
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public int getNumDocs() {
            return afd.getNumDocs();
        }

        @Override
        public long getNumberUniqueValues() {
            return afd.getNumberUniqueValues();
        }

        @Override
        public long getMemorySizeInBytes() {
            return afd.getMemorySizeInBytes();
        }

        @Override
        public ScriptDocValues getScriptValues() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

    }

    private static class GlobalOrdinalsDocs implements Ordinals.Docs {

        protected final LongValues segmentOrdToGlobalOrdLookup;
        protected final Ordinals.Docs segmentOrdinals;
        private final long memorySizeInBytes;
        protected final long maxOrd;

        protected long currentGlobalOrd;

        private GlobalOrdinalsDocs(Ordinals.Docs segmentOrdinals, LongValues segmentOrdToGlobalOrdLookup, long memorySizeInBytes, long maxOrd) {
            this.segmentOrdinals = segmentOrdinals;
            this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            this.memorySizeInBytes = memorySizeInBytes;
            this.maxOrd = maxOrd;
        }

        @Override
        public Ordinals ordinals() {
            return new Ordinals() {
                @Override
                public long getMemorySizeInBytes() {
                    return memorySizeInBytes;
                }

                @Override
                public boolean isMultiValued() {
                    return GlobalOrdinalsDocs.this.isMultiValued();
                }

                @Override
                public int getNumDocs() {
                    return GlobalOrdinalsDocs.this.getNumDocs();
                }

                @Override
                public long getNumOrds() {
                    return GlobalOrdinalsDocs.this.getNumOrds();
                }

                @Override
                public long getMaxOrd() {
                    return GlobalOrdinalsDocs.this.getMaxOrd();
                }

                @Override
                public Docs ordinals() {
                    return GlobalOrdinalsDocs.this;
                }
            };
        }

        @Override
        public int getNumDocs() {
            return segmentOrdinals.getNumDocs();
        }

        @Override
        public long getNumOrds() {
            return maxOrd - Ordinals.MIN_ORDINAL;
        }

        @Override
        public long getMaxOrd() {
            return maxOrd;
        }

        @Override
        public boolean isMultiValued() {
            return segmentOrdinals.isMultiValued();
        }

        @Override
        public long getOrd(int docId) {
            long segmentOrd = segmentOrdinals.getOrd(docId);
            return currentGlobalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd);
        }

        @Override
        public LongsRef getOrds(int docId) {
            LongsRef refs = segmentOrdinals.getOrds(docId);
            for (int i = refs.offset; i < refs.length; i++) {
                refs.longs[i] = segmentOrdToGlobalOrdLookup.get(refs.longs[i]);
            }
            return refs;
        }

        @Override
        public long nextOrd() {
            long segmentOrd = segmentOrdinals.nextOrd();
            return currentGlobalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd);
        }

        @Override
        public int setDocument(int docId) {
            return segmentOrdinals.setDocument(docId);
        }

        @Override
        public long currentOrd() {
            return currentGlobalOrd;
        }
    }
}
