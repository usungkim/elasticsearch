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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 */
public class InternalGlobalOrdinalsBuilder extends AbstractIndexComponent implements GlobalOrdinalsBuilder {

    private final BigArrays bigArrays;

    public InternalGlobalOrdinalsBuilder(Index index, @IndexSettings Settings indexSettings, BigArrays bigArrays) {
        super(index, indexSettings);
        this.bigArrays = bigArrays;
    }

    @Override
    public IndexFieldData.WithOrdinals build(final IndexReader indexReader, IndexFieldData.WithOrdinals indexFieldData, Settings settings, CircuitBreakerService breakerService) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTime = System.currentTimeMillis();

        // It makes sense to make the overhead ratio configurable for the mapping from segment ords to global ords
        // However, other mappings are never the bottleneck and only used to get the original value from an ord, so
        // it makes sense to force COMPACT for them
        final float acceptableOverheadRatio = settings.getAsFloat("acceptable_overhead_ratio", PackedInts.FAST);
        final AppendingPackedLongBuffer globalOrdToFirstSegment = new AppendingPackedLongBuffer(PackedInts.COMPACT);
        globalOrdToFirstSegment.add(0);
        final MonotonicAppendingLongBuffer globalOrdToFirstSegmentOrd = new MonotonicAppendingLongBuffer(PackedInts.COMPACT);
        globalOrdToFirstSegmentOrd.add(0);

        // TODO: add enum
        String ordinalMappingType = settings.get("ordinal_mapping_type", "compressed");
        final OrdinalMappingBuilder ordinalMappingBuilder;
        if ("compressed".equals(ordinalMappingType)) {
            ordinalMappingBuilder = new CompressedBuilder(indexReader.leaves().size(), acceptableOverheadRatio);
        } else {
            throw new ElasticsearchIllegalArgumentException("Unsupported ordinal mapping type " + ordinalMappingType);
        }

        long currentGlobalOrdinal = 0;
        final AtomicFieldData.WithOrdinals[] withOrdinals = new AtomicFieldData.WithOrdinals[indexReader.leaves().size()];
        TermIterator termIterator = new TermIterator(indexFieldData, indexReader.leaves(), withOrdinals);
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
            currentGlobalOrdinal++;
            globalOrdToFirstSegment.add(termIterator.firstReaderIndex());
            globalOrdToFirstSegmentOrd.add(termIterator.firstLocalOrdinal());
            for (TermIterator.LeafSource leafSource : termIterator.competitiveLeafs()) {
                ordinalMappingBuilder.onOrdinal(leafSource.context.ord, currentGlobalOrdinal);
            }
        }

        long memorySizeInBytesCounter = 0;
        globalOrdToFirstSegment.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegment.ramBytesUsed();
        globalOrdToFirstSegmentOrd.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegmentOrd.ramBytesUsed();
        LongValues[] segmentOrdToGlobalOrdLookups = ordinalMappingBuilder.build();
        memorySizeInBytesCounter += ordinalMappingBuilder.getMemorySizeInBytes();

        final long memorySizeInBytes = memorySizeInBytesCounter;
        breakerService.getBreaker().addWithoutBreaking(memorySizeInBytes);

        if (logger.isDebugEnabled()) {
            logger.debug("Global ordinals loading for " + currentGlobalOrdinal + " values, took: " + (System.currentTimeMillis() - startTime) + " ms");
        }
        return new GlobalOrdinalsIndexFieldData(indexFieldData.index(), settings, indexFieldData.getFieldNames(), bigArrays, withOrdinals,
                globalOrdToFirstSegment, globalOrdToFirstSegmentOrd, segmentOrdToGlobalOrdLookups, memorySizeInBytes,
                currentGlobalOrdinal
        );
    }

    private interface OrdinalMappingBuilder {

        void onOrdinal(int readerIndex, long globalOrdinal);

        LongValues[] build();

        long getMemorySizeInBytes();

    }

    private class CompressedBuilder implements OrdinalMappingBuilder {

        final MonotonicAppendingLongBuffer[] segmentOrdToGlobalOrdLookups;
        long memorySizeInBytesCounter;

        private CompressedBuilder(int numSegments, float acceptableOverheadRatio) {
            segmentOrdToGlobalOrdLookups = new MonotonicAppendingLongBuffer[numSegments];
            for (int i = 0; i < segmentOrdToGlobalOrdLookups.length; i++) {
                segmentOrdToGlobalOrdLookups[i] = new MonotonicAppendingLongBuffer(acceptableOverheadRatio);
                segmentOrdToGlobalOrdLookups[i].add(0);
            }
        }

        public void onOrdinal(int readerIndex, long globalOrdinal) {
            segmentOrdToGlobalOrdLookups[readerIndex].add(globalOrdinal);
        }

        public LongValues[] build() {
            for (MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup : segmentOrdToGlobalOrdLookups) {
                segmentOrdToGlobalOrdLookup.freeze();
                memorySizeInBytesCounter += segmentOrdToGlobalOrdLookup.ramBytesUsed();
            }
            return segmentOrdToGlobalOrdLookups;
        }

        public long getMemorySizeInBytes() {
            return memorySizeInBytesCounter;
        }

    }

    private final static class TermIterator implements BytesRefIterator {

        private final List<LeafSource> leafSources;

        private final IntArrayList sourceSlots;
        private final IntArrayList competitiveSlots;
        private BytesRef currentTerm;

        private TermIterator(IndexFieldData.WithOrdinals indexFieldData, List<AtomicReaderContext> leaves, AtomicFieldData.WithOrdinals[] withOrdinals) throws IOException {
            this.leafSources = new ArrayList<>(leaves.size());
            this.sourceSlots = IntArrayList.newInstanceWithCapacity(leaves.size());
            this.competitiveSlots = IntArrayList.newInstanceWithCapacity(leaves.size());
            for (int i = 0; i < leaves.size(); i++) {
                AtomicReaderContext leaf = leaves.get(i);
                AtomicFieldData.WithOrdinals afd = indexFieldData.load(leaf);
                withOrdinals[i] = afd;
                leafSources.add(new LeafSource(leaf, afd));
            }
        }

        public BytesRef next() throws IOException {
            if (currentTerm == null) {
                for (int slot = 0; slot < leafSources.size(); slot++) {
                    LeafSource leafSource = leafSources.get(slot);
                    if (leafSource.next() != null) {
                        sourceSlots.add(slot);
                    }
                }
            }
            if (sourceSlots.isEmpty()) {
                return null;
            }

            if (!competitiveSlots.isEmpty()) {
                for (IntCursor cursor : competitiveSlots) {
                    if (leafSources.get(cursor.value).next() == null) {
                        sourceSlots.removeFirstOccurrence(cursor.value);
                    }
                }
                competitiveSlots.clear();
            }
            BytesRef lowest = null;
            for (IntCursor cursor : sourceSlots) {
                LeafSource leafSource = leafSources.get(cursor.value);
                if (lowest == null) {
                    lowest = leafSource.currentTerm;
                    competitiveSlots.add(cursor.value);
                } else {
                    int cmp = lowest.compareTo(leafSource.currentTerm);
                    if (cmp == 0) {
                        competitiveSlots.add(cursor.value);
                    } else if (cmp > 0) {
                        competitiveSlots.clear();
                        lowest = leafSource.currentTerm;
                        competitiveSlots.add(cursor.value);
                    }
                }
            }

            if (competitiveSlots.isEmpty()) {
                return currentTerm = null;
            } else {
                return currentTerm = lowest;
            }
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }

        List<LeafSource> competitiveLeafs() throws IOException {
            List<LeafSource> docsEnums = new ArrayList<LeafSource>(competitiveSlots.size());
            for (IntCursor cursor : competitiveSlots) {
                LeafSource leafSource = leafSources.get(cursor.value);
                docsEnums.add(leafSource);
            }
            return docsEnums;
        }

        int firstReaderIndex() {
            int slot = competitiveSlots.get(0);
            return leafSources.get(slot).context.ord;
        }

        long firstLocalOrdinal() {
            int slot = competitiveSlots.get(0);
            return leafSources.get(slot).currentLocalOrd;
        }

        private static class LeafSource {

            final AtomicReaderContext context;
            final BytesValues.WithOrdinals afd;
            final long localMaxOrd;

            long currentLocalOrd = Ordinals.MISSING_ORDINAL;
            BytesRef currentTerm;

            private LeafSource(AtomicReaderContext context, AtomicFieldData.WithOrdinals afd) throws IOException {
                this.context = context;
                this.afd = afd.getBytesValues(false);
                this.localMaxOrd = this.afd.ordinals().getMaxOrd();
            }

            BytesRef next() throws IOException {
                if (++currentLocalOrd < localMaxOrd) {
                    return currentTerm = afd.getValueByOrd(currentLocalOrd);
                } else {
                    return null;
                }
            }

        }

    }
}
