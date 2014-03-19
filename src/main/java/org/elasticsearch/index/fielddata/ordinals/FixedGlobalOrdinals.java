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
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 */
public class FixedGlobalOrdinals implements GlobalOrdinalsBuilder {

    @Override
    public IndexFieldData.WithOrdinals build(final IndexReader indexReader, IndexFieldData.WithOrdinals indexFieldData, Settings settings, CircuitBreakerService breakerService) throws IOException {
        final float acceptableOverheadRatio = settings.getAsFloat("acceptable_overhead_ratio", PackedInts.FASTEST);
        final AppendingPackedLongBuffer globalOrdToFirstSegment = new AppendingPackedLongBuffer(acceptableOverheadRatio);
        globalOrdToFirstSegment.add(0);
        final MonotonicAppendingLongBuffer globalOrdToFirstSegmentOrd = new MonotonicAppendingLongBuffer(acceptableOverheadRatio);
        globalOrdToFirstSegmentOrd.add(0);
        final MonotonicAppendingLongBuffer[] segmentOrdToGlobalOrds = new MonotonicAppendingLongBuffer[indexReader.leaves().size()];
        for (int i = 0; i < segmentOrdToGlobalOrds.length; i++) {
            segmentOrdToGlobalOrds[i] = new MonotonicAppendingLongBuffer(acceptableOverheadRatio);
            segmentOrdToGlobalOrds[i].add(0);
        }

        long currentGlobalOrdinal = 0;
        final AtomicFieldData.WithOrdinals[] withOrdinals = new AtomicFieldData.WithOrdinals[indexReader.leaves().size()];
        TermIterator termIterator = new TermIterator(indexFieldData, indexReader.leaves(), withOrdinals);
        for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
            currentGlobalOrdinal++;
            globalOrdToFirstSegment.add(termIterator.firstReaderIndex());
            globalOrdToFirstSegmentOrd.add(termIterator.firstLocalOrdinal());
            for (TermIterator.LeafSource leafSource : termIterator.competitiveLeafs()) {
                segmentOrdToGlobalOrds[leafSource.context.ord].add(currentGlobalOrdinal);
            }
        }

        long memorySizeInBytesCounter = 0;
        globalOrdToFirstSegment.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegment.ramBytesUsed();
        globalOrdToFirstSegmentOrd.freeze();
        memorySizeInBytesCounter += globalOrdToFirstSegmentOrd.ramBytesUsed();
        for (MonotonicAppendingLongBuffer segmentOrdToGlobalOrd : segmentOrdToGlobalOrds) {
            segmentOrdToGlobalOrd.freeze();
            memorySizeInBytesCounter += segmentOrdToGlobalOrd.ramBytesUsed();
        }
        final long memorySizeInBytes = memorySizeInBytesCounter;
        breakerService.getBreaker().addWithoutBreaking(memorySizeInBytes);

        return new BaseGlobalIndexFieldData(indexFieldData.index(), settings, indexFieldData.getFieldNames(), withOrdinals) {

            @Override
            protected BytesRef getValueByGlobalOrd(long globalOrdinal) {
                assert globalOrdinal != Ordinals.MISSING_ORDINAL;

                int readerIndex = (int) globalOrdToFirstSegment.get(globalOrdinal);
                AtomicFieldData.WithOrdinals afd = withOrdinals[readerIndex];
                long segmentOrd =  globalOrdToFirstSegmentOrd.get(globalOrdinal);
                return afd.getBytesValues(false).getValueByOrd(segmentOrd);
            }

            @Override
            protected int getReaderIndex(long globalOrdinal) {
                assert globalOrdinal != Ordinals.MISSING_ORDINAL;
                return (int) globalOrdToFirstSegment.get(globalOrdinal);
            }

            @Override
            protected long getSegmentOrdinal(long globalOrdinal) {
                assert globalOrdinal != Ordinals.MISSING_ORDINAL;
                return globalOrdToFirstSegmentOrd.get(globalOrdinal);
            }

            @Override
            protected BytesValues.WithOrdinals createSegmentBytesValues(int readerIndex) {
                return withOrdinals[readerIndex].getBytesValues(false);
            }

            @Override
            protected long getGlobalOrd(int readerIndex, long segmentOrd) {
                return segmentOrdToGlobalOrds[readerIndex].get(segmentOrd);
            }

            public long getMemorySizeInBytes() {
                return memorySizeInBytes;
            }

        };
    }

    private final static class TermIterator implements BytesRefIterator {

        private final List<LeafSource> leafSources;

        private final IntArrayList sourceSlots;
        private final IntArrayList competitiveSlots;
        private BytesRef currentTerm;

        private TermIterator(IndexFieldData.WithOrdinals indexFieldData, List<AtomicReaderContext> leaves, AtomicFieldData.WithOrdinals[] withOrdinals) throws IOException {
            this.leafSources = new ArrayList<LeafSource>(leaves.size());
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
