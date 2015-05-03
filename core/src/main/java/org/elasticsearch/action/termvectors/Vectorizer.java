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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
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

    private ArrayList<Term> terms;
    private int size;
    private Map<String, ValueOption> valueOptions;
    private CoordQ coordQ = null;
    
    public Vectorizer() {
        
    }
    
    public Vectorizer(ArrayList<Term> terms, Map<String, ValueOption> valueOptions) {
        this.terms = terms;
        this.size = terms.size();
        this.valueOptions = new HashMap<>();
        for (String fieldName : valueOptions.keySet()) {
            setValueOption(fieldName, valueOptions.get(fieldName));
        }
        this.coordQ = new CoordQ(size);
    }

    public int size() {
        return size;
    }

    public void setValueOption(String fieldName, ValueOption valueOption) {
        this.valueOptions.put(fieldName, valueOption);
    }

    public static Vectorizer parse(XContentParser parser) throws IOException {
        ArrayList<Term> terms = new ArrayList<>();
        Map<String, ValueOption> valueOptions = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            parseTerms(parser, terms, valueOptions);
        }
        return new Vectorizer(terms, valueOptions);
    }

    private static void parseTerms(XContentParser parser, ArrayList<Term> terms, Map<String, ValueOption> valueOptions) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String fieldName = null;
        Set<String> words = new LinkedHashSet<>();  // to remove duplicates but preserve order
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
        for (int i = 0; i < size; i++) {
            String fieldName = in.readString();
            BytesRef termBytesRef = in.readBytesRef();
            terms.add(new Term(fieldName, termBytesRef));
        }
        valueOptions = new HashMap<>();
        for (int i = 0; i < in.readVInt(); i++) {
            String fieldName = in.readString();
            ValueOption valueOption = parseValueOption(in.readString());
            valueOptions.put(fieldName, valueOption);
        }
        coordQ = new CoordQ(size);
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
            out.writeString(valueOptions.get(fieldName).name());
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
        if (termStatistics == null) {  // term statistics were not requested!
            return -1;
        }
        switch (valueOptions.get(fieldName)) {
            case DOC_FREQ:
                return (int) termStatistics.docFreq();
            case TTF:
                return (int) termStatistics.totalTermFreq();
            default:
                return freq;  // default
        }
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

        private BytesStreamInput vectorInput;
        private int shape = 0;
        private int maxSize = 0;
        private int currentColumn = 0;
        
        public SparseVector() {
        }

        public SparseVector(BytesReference vectorInput) throws IOException {
            this.vectorInput = new BytesStreamInput(vectorInput);
            this.shape = this.vectorInput.readVInt();
            this.maxSize = this.vectorInput.readVInt();
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
