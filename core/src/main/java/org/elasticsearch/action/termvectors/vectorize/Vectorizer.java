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

package org.elasticsearch.action.termvectors.vectorize;

import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

public class Vectorizer {

    public static final SparseVector EMPTY_SPARSE_VECTOR = new SparseVector();

    public static enum ValueOption {
        TERM_FREQ, DOC_FREQ, TTF
    }

    private List<Term> terms;
    private int size;
    private Map<String, ValueOption> valueOptions;
    private CoordQ coordQ = null;
    
    public Vectorizer() {
        
    }
    
    public Vectorizer(List<Term> terms, Map<String, ValueOption> valueOptions) {
        LinkedHashSet<Term> uniqueTerms = new LinkedHashSet<>(terms); // remove duplicates
        this.terms = Lists.newArrayList(uniqueTerms);
        this.size = this.terms.size();
        this.valueOptions = valueOptions;
        this.coordQ = new CoordQ(size);
    }

    public int size() {
        return size;
    }

    public void setValueOption(String fieldName, ValueOption valueOption) {
        this.valueOptions.put(fieldName, valueOption);
    }

    public static Vectorizer parse(XContentParser parser) throws IOException {
        List<Term> terms = new ArrayList<>();
        Map<String, ValueOption> valueOptions = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            parseTerms(parser, terms, valueOptions);
        }
        return new Vectorizer(terms, valueOptions);
    }

    private static void parseTerms(XContentParser parser, List<Term> terms, Map<String, ValueOption> valueOptions) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String fieldName = null;
        List<String> words = new ArrayList<>();
        ValueOption valueOption = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName != null) {
                if (currentFieldName.equals("field")) {
                    fieldName = parser.text();
                } else if (currentFieldName.equals("span")) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            words.add(parser.text());
                        }
                    } else {
                        throw new ElasticsearchParseException("The parameter span must be given as an array!");
                    }
                } else if (currentFieldName.equals("value")) {
                    valueOption = parseValueOption(parser.text());
                } else {
                    throw new ElasticsearchParseException("The parameter " + currentFieldName + " is not valid for a vectorizer!");
                }
            }
        }
        if (fieldName == null) {
            throw new ElasticsearchParseException("The parameter " + fieldName + " is required!");
        }
        for (String word : words) {  //todo: inefficient but parsing may change with field name as key 
            terms.add(new Term(fieldName, word));
        }
        valueOptions.put(fieldName, valueOption);
    }

    private static ValueOption parseValueOption(String text) {
        switch (text) {
            case "term_freq":
                return ValueOption.TERM_FREQ;
            case "doc_freq":
                return ValueOption.DOC_FREQ;
            case "ttf":
                return ValueOption.TTF;
            default:
                throw new ElasticsearchParseException("The parameter value " + text + " is not valid!");
        }
    }

    public void readFrom(StreamInput in) throws IOException {
        this.size = in.readVInt();
        this.terms = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String fieldName = in.readString();
            BytesRef termBytesRef = in.readBytesRef();
            terms.add(new Term(fieldName, termBytesRef));
        }
        int numOptions = in.readVInt();
        this.valueOptions = new HashMap<>(numOptions);
        for (int i = 0; i < numOptions; i++) {
            String fieldName = in.readString();
            ValueOption valueOption = parseValueOption(in.readString());
            valueOptions.put(fieldName, valueOption);
        }
        this.coordQ = new CoordQ(size);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        for (Term term : terms) {
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
        }
        out.writeVInt(valueOptions.size());
        for (String fieldName : valueOptions.keySet()) {
            out.writeString(fieldName);
            String valueOption = valueOptions.get(fieldName).name().toLowerCase();
            out.writeString(valueOption);
        }
    }
    
    public void add(Term term, TermStatistics termStatistics, int freq) {
        int column = getColumn(term);
        if (column != -1) {
            coordQ.add(new Coord(column, getValue(term.field(), termStatistics, freq)));
        }
    }

    public int getColumn(Term term) {
        return terms.indexOf(term);
    }

    private int getValue(String fieldName, @Nullable TermStatistics termStatistics, int freq) {
        ValueOption valueOption = valueOptions.get(fieldName);
        if (valueOption == ValueOption.DOC_FREQ) {
            return termStatistics != null ? (int) termStatistics.docFreq() : -1;  // -1 term stats not requested
        } else if (valueOption == ValueOption.TTF) {
            return termStatistics != null ? (int) termStatistics.totalTermFreq() : -1;  // -1 term stats not requested
        }
        return freq; // default
    }

    public static SparseVector readVector(BytesReference vector) throws IOException {
        return new SparseVector(vector);
    }

    public BytesReference writeVector() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        Coord coord;
        output.writeVInt(size);
        output.writeVInt(coordQ.size());
        while ((coord = coordQ.pop()) != null) {
            output.writeVInt(coord.x);
            output.writeVInt(coord.y);
        }
        output.close();
        return output.bytes();
    }

    private static class Coord {
        public int x;
        public int y;
        
        public Coord(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    private static class CoordQ extends PriorityQueue<Coord> {
        public CoordQ(int maxSize) {
            super(maxSize, false);
        }

        @Override
        protected boolean lessThan(Coord a, Coord b) {
            return a.x < b.x;
        }
    }

    public static class SparseVector implements Iterator<Coord>, ToXContent {

        private StreamInput vectorInput;
        private int shape = 0;
        private int maxSize = 0;
        private int currentColumn = 0;
        
        public SparseVector() {
        }

        public SparseVector(BytesReference vectorInput) throws IOException {
            this.vectorInput = StreamInput.wrap(vectorInput);
            reset();
        }

        private void reset() throws IOException {
            this.vectorInput.reset();
            this.shape = this.vectorInput.readVInt();
            this.maxSize = this.vectorInput.readVInt();
            this.currentColumn = 0;
        }
        
        public int getShape() {
            return this.shape;
        }
        
        public int getMaxSize() {
            return this.maxSize;
        }
        
        public Tuple<int[], double[]> getIndicesAndValues() throws IOException {
            reset();
            int[] indices = new int[maxSize];
            double[] values = new double[maxSize];
            int i = 0;
            while (hasNext()) {
                Coord coord = next();
                indices[i] = coord.x;
                values[i] = coord.y;
                i++;
            }
            return new Tuple<>(indices, values);
        }

        @Override
        public boolean hasNext() {
            return currentColumn < maxSize;
        }
        
        @Override
        public Coord next() {
            try {
                int x = vectorInput.readVInt();
                int y = vectorInput.readVInt();
                currentColumn++;
                return new Coord(x, y);
            } catch (IOException e) {
                throw new ElasticsearchException("unable to read coordinate from stream!");
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            reset();
            
            builder.field("shape", shape);
            builder.startArray("vector");
            while (hasNext()) {
                Coord coord = next();
                builder.startObject();
                builder.field(String.valueOf(coord.x), coord.y);
                builder.endObject();
            }
            builder.endArray();
            return builder;
        }
    }
}
