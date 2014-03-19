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
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;

/**
 * Base class for global ordinal consumption.
 */
public abstract class BaseGlobalIndexFieldData extends AbstractIndexComponent implements IndexFieldData.WithOrdinals, RamUsage {

    private final FieldMapper.Names fieldNames;
    private final Atomic[] atomicReaders;

    public BaseGlobalIndexFieldData(Index index, Settings settings, FieldMapper.Names fieldNames, AtomicFieldData.WithOrdinals[] segmentAfd) {
        super(index, settings);
        this.fieldNames = fieldNames;
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(i, segmentAfd[i]);
        }
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
    public boolean hasGlobalOrdinals() {
        return true;
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

    protected abstract int getReaderIndex(long globalOrdinal);

    protected abstract long getSegmentOrdinal(long globalOrdinal);

    protected abstract BytesValues.WithOrdinals createSegmentBytesValues(int readerIndex);

    // TODO: should be removed...
    protected abstract BytesRef getValueByGlobalOrd(long globalOrd);

    protected abstract long getGlobalOrd(int readerIndex, long segmentOrd);

    private class Atomic implements AtomicFieldData.WithOrdinals {

        private final int readerIndex;
        private final AtomicFieldData.WithOrdinals afd;

        private Atomic(int readerIndex, WithOrdinals afd) {
            this.readerIndex = readerIndex;
            this.afd = afd;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
            BytesValues.WithOrdinals values = afd.getBytesValues(false);
            Ordinals.Docs wrapper = new SegmentOrdinalsToGlobalOrdinalsWrapper(values.ordinals());
            return new BytesValues.WithOrdinals(wrapper) {

                int readerIndex;
                final IntObjectOpenHashMap<BytesValues.WithOrdinals> bytesValuesCache = new IntObjectOpenHashMap<>();

                @Override
                public BytesRef getValueByOrd(long globalOrd) {
                    final long segmentOrd = getSegmentOrdinal(globalOrd);
                    readerIndex = getReaderIndex(globalOrd);
                    if (bytesValuesCache.containsKey(readerIndex)) {
                        return bytesValuesCache.lget().getValueByOrd(segmentOrd);
                    } else {
                        BytesValues.WithOrdinals k = createSegmentBytesValues(readerIndex);
                        bytesValuesCache.put(readerIndex, k);
                        return k.getValueByOrd(segmentOrd);
                    }
//                    return getValueByGlobalOrd(globalOrd);
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

        private class SegmentOrdinalsToGlobalOrdinalsWrapper implements Ordinals.Docs {

            private final Ordinals.Docs segmentOrdinals;
            private long currentGlobalOrd;

            private SegmentOrdinalsToGlobalOrdinalsWrapper(Ordinals.Docs segmentOrdinals) {
                this.segmentOrdinals = segmentOrdinals;
            }

            @Override
            public Ordinals ordinals() {
                return new Ordinals() {
                    @Override
                    public long getMemorySizeInBytes() {
                        return BaseGlobalIndexFieldData.this.getMemorySizeInBytes();
                    }

                    @Override
                    public boolean isMultiValued() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.isMultiValued();
                    }

                    @Override
                    public int getNumDocs() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.getNumDocs();
                    }

                    @Override
                    public long getNumOrds() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.getNumOrds();
                    }

                    @Override
                    public long getMaxOrd() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.getMaxOrd();
                    }

                    @Override
                    public Docs ordinals() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this;
                    }
                };
            }

            @Override
            public int getNumDocs() {
                return segmentOrdinals.getNumDocs();
            }

            @Override
            public long getNumOrds() {
                return segmentOrdinals.getNumOrds();
            }

            @Override
            public long getMaxOrd() {
                return segmentOrdinals.getMaxOrd();
            }

            @Override
            public boolean isMultiValued() {
                return segmentOrdinals.isMultiValued();
            }

            @Override
            public long getOrd(int docId) {
                long segmentOrd = segmentOrdinals.getOrd(docId);
                return currentGlobalOrd = getGlobalOrd(readerIndex, segmentOrd);
            }

            @Override
            public LongsRef getOrds(int docId) {
                LongsRef refs = segmentOrdinals.getOrds(docId);
                for (int i = refs.offset; i < refs.length; i++) {
                    refs.longs[i] = getGlobalOrd(readerIndex, refs.longs[i]);
                }
                return refs;
            }

            @Override
            public long nextOrd() {
                long segmentOrd = segmentOrdinals.nextOrd();
                return currentGlobalOrd = getGlobalOrd(readerIndex, segmentOrd);
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
}
