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

package org.elasticsearch.action.termvectors;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest.Flag;
import org.elasticsearch.action.termvectors.vectorize.Vectorizer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

public class TermVectorsResponse extends ActionResponse implements ToXContent {

    private static class FieldStrings {
        // term statistics strings
        public static final XContentBuilderString TTF = new XContentBuilderString("ttf");
        public static final XContentBuilderString DOC_FREQ = new XContentBuilderString("doc_freq");
        public static final XContentBuilderString TERM_FREQ = new XContentBuilderString("term_freq");
        public static final XContentBuilderString SCORE = new XContentBuilderString("score");

        // field statistics strings
        public static final XContentBuilderString FIELD_STATISTICS = new XContentBuilderString("field_statistics");
        public static final XContentBuilderString DOC_COUNT = new XContentBuilderString("doc_count");
        public static final XContentBuilderString SUM_DOC_FREQ = new XContentBuilderString("sum_doc_freq");
        public static final XContentBuilderString SUM_TTF = new XContentBuilderString("sum_ttf");

        public static final XContentBuilderString TOKENS = new XContentBuilderString("tokens");
        public static final XContentBuilderString POS = new XContentBuilderString("position");
        public static final XContentBuilderString START_OFFSET = new XContentBuilderString("start_offset");
        public static final XContentBuilderString END_OFFSET = new XContentBuilderString("end_offset");
        public static final XContentBuilderString PAYLOAD = new XContentBuilderString("payload");
        public static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        public static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        public static final XContentBuilderString _ID = new XContentBuilderString("_id");
        public static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        public static final XContentBuilderString FOUND = new XContentBuilderString("found");
        public static final XContentBuilderString TOOK = new XContentBuilderString("took");
        public static final XContentBuilderString TERMS = new XContentBuilderString("terms");
        public static final XContentBuilderString TERM_VECTORS = new XContentBuilderString("term_vectors");
        public static final XContentBuilderString VECTOR = new XContentBuilderString("vector");
    }

    private BytesReference termVectors;
    private BytesReference vector;
    private BytesReference headerRef;
    private String index;
    private String type;
    private String id;
    private long docVersion;
    private boolean exists = false;
    private boolean artificial = false;
    private long tookInMillis;
    private boolean hasScores = false;

    private boolean sourceCopied = false;

    int[] currentPositions = new int[0];
    int[] currentStartOffset = new int[0];
    int[] currentEndOffset = new int[0];
    BytesReference[] currentPayloads = new BytesReference[0];

    public TermVectorsResponse(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public TermVectorsResponse() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeVLong(docVersion);
        final boolean docExists = isExists();
        out.writeBoolean(docExists);
        out.writeBoolean(artificial);
        out.writeVLong(tookInMillis);
        out.writeBoolean(hasTermVectors());
        if (hasTermVectors()) {
            out.writeBytesReference(headerRef);
            out.writeBytesReference(termVectors);
        }
        out.writeBoolean(hasVector());
        if (hasVector()) {
            out.writeBytesReference(vector);
        }
    }

    private boolean hasTermVectors() {
        assert (headerRef == null && termVectors == null) || (headerRef != null && termVectors != null);
        return headerRef != null;
    }

    private boolean hasVector() {
        return vector != null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        type = in.readString();
        id = in.readString();
        docVersion = in.readVLong();
        exists = in.readBoolean();
        artificial = in.readBoolean();
        tookInMillis = in.readVLong();
        if (in.readBoolean()) {
            headerRef = in.readBytesReference();
            termVectors = in.readBytesReference();
        }
        if (in.readBoolean()) {
            vector = in.readBytesReference();
        }
    }

    public Fields getFields() throws IOException {
        if (hasTermVectors() && isExists()) {
            if (!sourceCopied) { // make the bytes safe
                headerRef = headerRef.copyBytesArray();
                termVectors = termVectors.copyBytesArray();
            }
            TermVectorsFields termVectorsFields = new TermVectorsFields(headerRef, termVectors);
            hasScores = termVectorsFields.hasScores;
            return termVectorsFields;
        } else {
            return new Fields() {
                @Override
                public Iterator<String> iterator() {
                    return Collections.emptyIterator();
                }

                @Override
                public Terms terms(String field) throws IOException {
                    return null;
                }

                @Override
                public int size() {
                    return 0;
                }
            };
        }
    }
    
    public Vectorizer.SparseVector getVector() throws IOException {
        if (hasVector()) {
            return Vectorizer.readVector(vector);
        } else {
            return Vectorizer.EMPTY_SPARSE_VECTOR;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert index != null;
        assert type != null;
        assert id != null;
        builder.field(FieldStrings._INDEX, index);
        builder.field(FieldStrings._TYPE, type);
        if (!isArtificial()) {
            builder.field(FieldStrings._ID, id);
        }
        builder.field(FieldStrings._VERSION, docVersion);
        builder.field(FieldStrings.FOUND, isExists());
        builder.field(FieldStrings.TOOK, tookInMillis);
        if (isExists()) {
            buildTermVectors(builder);
        }
        return builder;
    }
    
    public void buildTermVectors(XContentBuilder builder) throws IOException {
        builder.startObject(FieldStrings.TERM_VECTORS);
        final CharsRefBuilder spare = new CharsRefBuilder();
        Fields theFields = getFields();
        Iterator<String> fieldIter = theFields.iterator();
        while (fieldIter.hasNext()) {
            buildField(builder, spare, theFields, fieldIter);
        }
        builder.endObject();
    }

    private void buildField(XContentBuilder builder, final CharsRefBuilder spare, Fields theFields, Iterator<String> fieldIter) throws IOException {
        String fieldName = fieldIter.next();
        builder.startObject(fieldName);
        Terms curTerms = theFields.terms(fieldName);
        // write field statistics
        buildFieldStatistics(builder, curTerms);
        builder.startObject(FieldStrings.TERMS);
        TermsEnum termIter = curTerms.iterator();
        BoostAttribute boostAtt = termIter.attributes().addAttribute(BoostAttribute.class);
        for (int i = 0; i < curTerms.size(); i++) {
            buildTerm(builder, spare, curTerms, termIter, boostAtt);
        }
        builder.endObject();
        builder.endObject();
    }

    private void buildTerm(XContentBuilder builder, final CharsRefBuilder spare, Terms curTerms, TermsEnum termIter, BoostAttribute boostAtt) throws IOException {
        // start term, optimized writing
        BytesRef term = termIter.next();
        spare.copyUTF8Bytes(term);
        builder.startObject(spare.toString());
        buildTermStatistics(builder, termIter);
        // finally write the term vectors
        PostingsEnum posEnum = termIter.postings(null, null, PostingsEnum.ALL);
        int termFreq = posEnum.freq();
        builder.field(FieldStrings.TERM_FREQ, termFreq);
        initMemory(curTerms, termFreq);
        initValues(curTerms, posEnum, termFreq);
        buildValues(builder, curTerms, termFreq);
        buildScore(builder, boostAtt);
        builder.endObject();
    }

    private void buildTermStatistics(XContentBuilder builder, TermsEnum termIter) throws IOException {
        // write term statistics. At this point we do not naturally have a
        // boolean that says if these values actually were requested.
        // However, we can assume that they were not if the statistic values are
        // <= 0.
        assert (((termIter.docFreq() > 0) && (termIter.totalTermFreq() > 0)) || ((termIter.docFreq() == -1) && (termIter.totalTermFreq() == -1)));
        int docFreq = termIter.docFreq();
        if (docFreq > 0) {
            builder.field(FieldStrings.DOC_FREQ, docFreq);
            builder.field(FieldStrings.TTF, termIter.totalTermFreq());
        }
    }

    private void buildValues(XContentBuilder builder, Terms curTerms, int termFreq) throws IOException {
        if (!(curTerms.hasPayloads() || curTerms.hasOffsets() || curTerms.hasPositions())) {
            return;
        }

        builder.startArray(FieldStrings.TOKENS);
        for (int i = 0; i < termFreq; i++) {
            builder.startObject();
            if (curTerms.hasPositions()) {
                builder.field(FieldStrings.POS, currentPositions[i]);
            }
            if (curTerms.hasOffsets()) {
                builder.field(FieldStrings.START_OFFSET, currentStartOffset[i]);
                builder.field(FieldStrings.END_OFFSET, currentEndOffset[i]);
            }
            if (curTerms.hasPayloads() && (currentPayloads[i].length() > 0)) {
                builder.field(FieldStrings.PAYLOAD, currentPayloads[i]);
            }
            builder.endObject();
        }
        builder.endArray();
    }

    private void initValues(Terms curTerms, PostingsEnum posEnum, int termFreq) throws IOException {
        for (int j = 0; j < termFreq; j++) {
            int nextPos = posEnum.nextPosition();
            if (curTerms.hasPositions()) {
                currentPositions[j] = nextPos;
            }
            if (curTerms.hasOffsets()) {
                currentStartOffset[j] = posEnum.startOffset();
                currentEndOffset[j] = posEnum.endOffset();
            }
            if (curTerms.hasPayloads()) {
                BytesRef curPayload = posEnum.getPayload();
                if (curPayload != null) {
                    currentPayloads[j] = new BytesArray(curPayload.bytes, 0, curPayload.length);
                } else {
                    currentPayloads[j] = null;
                }
            }
        }
    }

    private void initMemory(Terms curTerms, int termFreq) {
        // init memory for performance reasons
        if (curTerms.hasPositions()) {
            currentPositions = ArrayUtil.grow(currentPositions, termFreq);
        }
        if (curTerms.hasOffsets()) {
            currentStartOffset = ArrayUtil.grow(currentStartOffset, termFreq);
            currentEndOffset = ArrayUtil.grow(currentEndOffset, termFreq);
        }
        if (curTerms.hasPayloads()) {
            currentPayloads = new BytesArray[termFreq];
        }
    }

    private void buildFieldStatistics(XContentBuilder builder, Terms curTerms) throws IOException {
        long sumDocFreq = curTerms.getSumDocFreq();
        int docCount = curTerms.getDocCount();
        long sumTotalTermFrequencies = curTerms.getSumTotalTermFreq();
        if (docCount > 0) {
            assert ((sumDocFreq > 0)) : "docCount >= 0 but sumDocFreq ain't!";
            assert ((sumTotalTermFrequencies > 0)) : "docCount >= 0 but sumTotalTermFrequencies ain't!";
            builder.startObject(FieldStrings.FIELD_STATISTICS);
            builder.field(FieldStrings.SUM_DOC_FREQ, sumDocFreq);
            builder.field(FieldStrings.DOC_COUNT, docCount);
            builder.field(FieldStrings.SUM_TTF, sumTotalTermFrequencies);
            builder.endObject();
        } else if (docCount == -1) { // this should only be -1 if the field
            // statistics were not requested at all. In
            // this case all 3 values should be -1
            assert ((sumDocFreq == -1)) : "docCount was -1 but sumDocFreq ain't!";
            assert ((sumTotalTermFrequencies == -1)) : "docCount was -1 but sumTotalTermFrequencies ain't!";
        } else {
            throw new IllegalStateException(
                    "Something is wrong with the field statistics of the term vector request: Values are " + "\n"
                            + FieldStrings.SUM_DOC_FREQ + " " + sumDocFreq + "\n" + FieldStrings.DOC_COUNT + " " + docCount + "\n"
                            + FieldStrings.SUM_TTF + " " + sumTotalTermFrequencies);
        }
    }

    public void updateTookInMillis(long startTime) {
        this.tookInMillis = Math.max(1, System.currentTimeMillis() - startTime);
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }
    
    private void buildScore(XContentBuilder builder, BoostAttribute boostAtt) throws IOException {
        if (hasScores) {
            builder.field(FieldStrings.SCORE, boostAtt.getBoost());
        }
    }

    public boolean isExists() {
        return exists;
    }
    
    public void setExists(boolean exists) {
         this.exists = exists;
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields) throws IOException {
        setFields(termVectorsByField, selectedFields, flags, topLevelFields, null, null, null);
    }

    public void setFields(Fields termVectorsByField, Set<String> selectedFields, EnumSet<Flag> flags, Fields topLevelFields, @Nullable AggregatedDfs dfs,
                          TermVectorsFilter termVectorsFilter, @Nullable Vectorizer vectorizer) throws IOException {
        TermVectorsWriter tvw = new TermVectorsWriter(this);

        if (termVectorsByField != null) {
            tvw.setFields(termVectorsByField, selectedFields, flags, topLevelFields, dfs, termVectorsFilter, vectorizer);
        }
    }

    public void setTermVectorsField(BytesStreamOutput output) {
        termVectors = output.bytes();
    }

    public void setVector(BytesReference output) {
        vector = output;
    }

    public void setHeader(BytesReference header) {
        headerRef = header;
    }

    public void setDocVersion(long version) {
        this.docVersion = version;

    }

    public Long getVersion() {
        return docVersion;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public boolean isArtificial() {
        return artificial;
    }

    public void setArtificial(boolean artificial) {
        this.artificial = artificial;
    }
}
