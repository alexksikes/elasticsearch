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

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.mapper.Uid.createUidAsBytes;

/**
 * A more like this query that finds documents that are "like" the provided {@link #likeText(String)}
 * which is checked against the fields the query is constructed with.
 */
public class MoreLikeThisQueryBuilder extends QueryBuilder implements Streamable, BoostableQueryBuilder<MoreLikeThisQueryBuilder> {

    /**
     * A single get item. Pure delegate to multi term vectors.
     */
    /** NO COMMIT: we should rather have an Item object in MultiTermVectorsRequest from which to extend to
     * and then only allow certain parameters of term vectors to be set?
     */
    public static final class Item extends TermVectorsRequest implements ToXContent {
        public static final Item[] EMPTY_ARRAY = new Item[0];

        private String likeText;

        public Item() {
            super();
            positions(false);
            offsets(false);
            payloads(false);
            fieldStatistics(false);
            termStatistics(false);
        }

        public Item(String index, String type, String id) {
            super(index, type, id);
        }

        public Item(String likeText) {
            this.likeText = likeText;
        }
        
        public boolean isLikeText() {
            return likeText != null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // NO COMMIT: some fields missing here, also move this to TermVectorRequest?
            if (this.likeText != null) {
                return builder.value(this.likeText);
            }
            builder.startObject();
            if (this.index() != null) {
                builder.field("_index", this.index());
            }
            if (this.type() != null) {
                builder.field("_type", this.type());
            }
            if (this.id() != null) {
                builder.field("_id", this.id());
            }
            if (this.doc() != null) {
                XContentType contentType = XContentFactory.xContentType(doc());
                if (contentType == builder.contentType()) {
                    builder.rawField("doc", doc());
                } else {
                    XContentParser parser = XContentFactory.xContent(contentType).createParser(doc());
                    parser.nextToken();
                    builder.field("doc");
                    builder.copyCurrentStructure(parser);
                }
            }
            if (this.selectedFields() != null) {
                builder.array("fields", this.selectedFields());
            }
            if (this.routing() != null) {
                builder.field("_routing", this.routing());
            }
            if (this.version() != Versions.MATCH_ANY) {
                builder.field("_version", this.version());
            }
            if (this.versionType() != VersionType.INTERNAL) {
                builder.field("_version_type", this.versionType().toString().toLowerCase(Locale.ROOT));
            }
            return builder.endObject();
        }

        public static Item readItem(StreamInput in) throws IOException {
            Item item = new Item();
            item.readFrom(in);
            return item;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                this.likeText = in.readString();
            } else {
                super.readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(isLikeText());
            if (isLikeText()) {
                out.writeString(likeText);
            } else {
                super.writeTo(out);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), likeText);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Item other = (Item) obj;
            return super.equals(other) && 
                    Objects.equals(likeText, other.likeText);
        }
    }

    private MoreLikeThisFetchService fetchService = null;

    private String[] fields;
    private List<Item> items = new ArrayList<>();
    private List<Item> ignoreItems = new ArrayList<>();
    private Boolean include;
    private String minimumShouldMatch = null;
    private int minTermFreq = -1;
    private int maxQueryTerms = -1;
    private String[] stopWords = null;
    private int minDocFreq = -1;
    private int maxDocFreq = -1;
    private int minWordLength = -1;
    private int maxWordLength = -1;
    private float boostTerms = 0;
    private String analyzer;
    private Boolean failOnUnsupportedField;
    private float boost = -1;
    private String queryName;

    /**
     * Constructs a new more like this query which uses the "_all" field.
     */
    public MoreLikeThisQueryBuilder() {
        this.fields = null;
    }

    /**
     * Sets the field names that will be used when generating the 'More Like This' query.
     *
     * @param fields the field names that will be used when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder(String... fields) {
        this.fields = fields;
    }

    @Inject(optional = true)
    public void setFetchService(@Nullable MoreLikeThisFetchService fetchService) {
            this.fetchService = fetchService;
    }

    public MoreLikeThisQueryBuilder fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    /**
     * Sets the documents to use in order to find documents that are "like" this.
     *
     * @param items the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder like(Item... items) {
        this.items = Arrays.asList(items);
        return this;
    }

    /**
     * Sets the text to use in order to find documents that are "like" this.
     *
     * @param likeText the text to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder like(String... likeText) {
        this.items = new ArrayList<>();
        for (String text : likeText) {
            this.items.add(new Item(text));
        }
        return this;
    }

    /**
     * Adds a document to use in order to find documents that are "like" this.
     */
    public MoreLikeThisQueryBuilder addItem(Item item) {
        this.items.add(item);
        return this;
    }

    /**
     * Adds some text to use in order to find documents that are "like" this.
     */
    public MoreLikeThisQueryBuilder addLikeText(String likeText) {
        this.items.add(new Item(likeText));
        return this;
    }

    /**
     * Sets the documents from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder ignoreLike(Item... items) {
        this.ignoreItems = Arrays.asList(items);
        return this;
    }

    /**
     * Sets the text from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder ignoreLike(String... likeText) {
        this.ignoreItems = new ArrayList<>();
        for (String text : likeText) {
            this.ignoreItems.add(new Item(text));
        }
        return this;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    @Deprecated
    public MoreLikeThisQueryBuilder likeText(String likeText) {
        return like(new Item(likeText));
    }

    @Deprecated
    public MoreLikeThisQueryBuilder ids(String... ids) {
        Item[] items = new Item[ids.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item(null, null, ids[i]);
        }
        return like(items);
    }

    @Deprecated
    public MoreLikeThisQueryBuilder docs(Item... items) {
        return like(items);
    }

    public MoreLikeThisQueryBuilder include(boolean include) {
        this.include = include;
        return this;
    }

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match. Defaults to <tt>30%</tt>.
     *
     * @see    org.elasticsearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public MoreLikeThisQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. The default
     * frequency is <tt>2</tt>.
     */
    public MoreLikeThisQueryBuilder minTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
        return this;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to <tt>25</tt>.
     */
    public MoreLikeThisQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * Set the set of stopwords.
     * <p/>
     * <p>Any word in this set is considered "uninteresting" and ignored. Even if your Analyzer allows stopwords, you
     * might want to tell the MoreLikeThis code to ignore them, as for the purposes of document similarity it seems
     * reasonable to assume that "a stop word is never interesting".
     */
    public MoreLikeThisQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisQueryBuilder minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisQueryBuilder maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * Sets the minimum word length below which words will be ignored. Defaults
     * to <tt>0</tt>.
     */
    public MoreLikeThisQueryBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Sets the maximum word length above which words will be ignored. Defaults to
     * unbounded (<tt>0</tt>).
     */
    public MoreLikeThisQueryBuilder maxWordLength(int maxWordLength) {
        this.maxWordLength = maxWordLength;
        return this;
    }

    /**
     * Sets the boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisQueryBuilder boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the fied.
     */
    public MoreLikeThisQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    @Override
    public MoreLikeThisQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }
    
    /**
     * Gets the boost for this query.
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public MoreLikeThisQueryBuilder failOnUnsupportedField(boolean fail) {
        failOnUnsupportedField = fail;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public MoreLikeThisQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the query.
     */
    public String queryName() {
        return this.queryName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        String likeFieldName = MoreLikeThisQueryParser.Fields.LIKE.getPreferredName();
        builder.startObject(MoreLikeThisQueryParser.NAME);
        if (fields != null) {
            builder.startArray("fields");
            for (String field : fields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (this.items.isEmpty()) {
            throw new IllegalArgumentException("more_like_this requires '" + likeFieldName + "' to be provided");
        } else {
            builder.field(likeFieldName, items);
        }
        if (!ignoreItems.isEmpty()) {
            builder.field(MoreLikeThisQueryParser.Fields.IGNORE_LIKE.getPreferredName(), ignoreItems);
        }
        if (minimumShouldMatch != null) {
            builder.field(MoreLikeThisQueryParser.Fields.MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }
        if (minTermFreq != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MIN_TERM_FREQ.getPreferredName(), minTermFreq);
        }
        if (maxQueryTerms != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MAX_QUERY_TERMS.getPreferredName(), maxQueryTerms);
        }
        if (stopWords != null && stopWords.length > 0) {
            builder.array(MoreLikeThisQueryParser.Fields.STOP_WORDS.getPreferredName(), stopWords);
        }
        if (minDocFreq != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        }
        if (maxDocFreq != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MAX_DOC_FREQ.getPreferredName(), maxDocFreq);
        }
        if (minWordLength != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        }
        if (maxWordLength != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MAX_WORD_LENGTH.getPreferredName(), maxWordLength);
        }
        if (boostTerms != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.BOOST_TERMS.getPreferredName(), boostTerms);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (failOnUnsupportedField != null) {
            builder.field(MoreLikeThisQueryParser.Fields.FAIL_ON_UNSUPPORTED_FIELD.getPreferredName(), failOnUnsupportedField);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (include != null) {
            builder.field("include", include);
        }
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws IOException {
        MoreLikeThisQuery query = new MoreLikeThisQuery();
        
        // first set some basic settings
        query.setSimilarity(parseContext.searchSimilarity());

        if (minTermFreq != -1) {
            query.setMinTermFrequency(minTermFreq);
        }
        if (maxQueryTerms != -1) {
            query.setMaxQueryTerms(maxQueryTerms);
        }
        if (minDocFreq != -1) {
            query.setMinDocFreq(minDocFreq);
        }
        if (maxDocFreq != -1) {
            query.setMaxDocFreq(maxDocFreq);
        }
        if (minWordLength != -1) {
            query.setMinWordLen(minWordLength);
        }
        if (maxWordLength != -1) {
            query.setMaxWordLen(maxWordLength);
        }
        if (boostTerms > 0) {
            query.setBoostTerms(true);
            query.setBoostTermsFactor(boostTerms);
        }
        query.setMinimumShouldMatch(minimumShouldMatch);

        // set analyzer
        if (analyzer != null) {
            query.setAnalyzer(parseContext.analysisService().analyzer(analyzer));
        } else {
            query.setAnalyzer(parseContext.mapperService().searchAnalyzer());
        }
        
        // set like text fields
        boolean useDefaultField = (fields == null);
        if (useDefaultField) {
            fields = new String[]{(parseContext.defaultField())};
        }
        query.setMoreLikeFields(fields);
        
        // prepare for requests and handle defaults
        MultiTermVectorsRequest requests = new MultiTermVectorsRequest();
        List<String> likeTexts = new ArrayList<>();
        List<String> ignoreTexts = new ArrayList<>();
        prepareRequest(requests, likeTexts, items, useDefaultField, parseContext);
        prepareRequest(requests, ignoreTexts, ignoreItems, useDefaultField, parseContext);
        
        // first take care of likeTexts
        if (!likeTexts.isEmpty()) {
            query.setLikeText(likeTexts);
        }
        if (!ignoreTexts.isEmpty()) {
            query.setIgnoreText(ignoreTexts);
        }
        
        // possibly remove unsupported fields when using like text items
        if (!likeTexts.isEmpty() || !ignoreTexts.isEmpty()) {
            fields = removeUnsupportedFields(Arrays.asList(fields), query.getAnalyzer(), failOnUnsupportedField);
        }
        
        // nothing to do in this case
        if (requests.isEmpty() && likeTexts.isEmpty() || !likeTexts.isEmpty() && fields.length == 0) {
            return Queries.newMatchNoDocsQuery();
        }

        // now we can fetch the actual items if there are any
        if (!requests.isEmpty()) {
            MultiTermVectorsResponse responses = fetchService.fetchResponse(requests);

            // getting the Fields for liked items
            query.setLikeText(MoreLikeThisFetchService.getFields(responses, items));

            // getting the Fields for ignored items
            if (!ignoreItems.isEmpty()) {
                Fields[] ignoreFields = MoreLikeThisFetchService.getFields(responses, ignoreItems);
                if (ignoreFields.length > 0) {
                    query.setIgnoreText(ignoreFields);
                }
            }

            // exclude the items from the search
            if (include == null || !include) {
                handleExclude(query, requests);
            }
        }

        // setting other meta parameters
        query.setBoost(this.boost);
        if (this.queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    private void prepareRequest(MultiTermVectorsRequest requests, List<String> likeTexts, List<Item> items, boolean useDefaultField, QueryParseContext parseContext) {
        // set default index, type and fields if not specified
        for (Item item : items) {
            // handle like text items
            if (item.isLikeText()) {
                likeTexts.add(item.likeText);
                continue;
            }
            if (item.index() == null) {
                item.index(parseContext.index().name());
            }
            if (item.type() == null) {
                if (parseContext.queryTypes().size() > 1) {
                    throw new QueryParsingException(parseContext, "ambiguous type for item with id: " + item.id()
                            + " and index: " + item.index());
                } else {
                    item.type(parseContext.queryTypes().iterator().next());
                }
            }
            // default fields if not present but don't override for artificial docs
            if (item.selectedFields() == null && item.doc() == null) {
                if (useDefaultField) {
                    item.selectedFields("*");
                } else {
                    item.selectedFields(fields);
                }
            }
            requests.add(item);
        }
    }

    private String[] removeUnsupportedFields(List<String> moreLikeFields, Analyzer analyzer, @Nullable Boolean failOnUnsupportedField) throws IOException {
        for (Iterator<String> it = moreLikeFields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField != null || failOnUnsupportedField) {
                    throw new IllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        return moreLikeFields.toArray(Strings.EMPTY_ARRAY);
    }
    
    private void handleExclude(MoreLikeThisQuery mltQuery, MultiTermVectorsRequest likeItems) {
        // artificial docs get assigned a random id and should be disregarded
        List<BytesRef> uids = new ArrayList<>();
        for (TermVectorsRequest item : likeItems) {
            if (item.doc() != null) {
                continue;
            }
            uids.add(createUidAsBytes(item.type(), item.id()));
        }
        if (!uids.isEmpty()) {
            mltQuery.setExclude(uids.toArray(new BytesRef[0]));
        }
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (items == null || items.isEmpty()) {
            validationException = QueryValidationException.addValidationError("more_like_this requires 'like' to be specified", validationException);
        }
        if (fields != null && fields.length == 0) {
            validationException = QueryValidationException.addValidationError("more_like_this requires 'fields' to be non-empty", validationException);
        }
        return validationException;
    }

    @Override
    protected String parserName() {
        return MoreLikeThisQueryParser.NAME;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        fields = in.readOptionalStringArray();
        
        int size = in.readVInt();
        items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(Item.readItem(in));
        }
        size = in.readVInt();
        ignoreItems = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ignoreItems.add(Item.readItem(in));
        }

        include = in.readOptionalBoolean();
        minimumShouldMatch = in.readOptionalString();
        minTermFreq = in.readVInt();
        maxQueryTerms = in.readVInt();
        stopWords = in.readOptionalStringArray();
        minDocFreq = in.readVInt();
        maxDocFreq = in.readVInt();
        minWordLength = in.readVInt();
        maxWordLength = in.readVInt();
        boostTerms = in.readFloat();
        analyzer = in.readOptionalString();
        failOnUnsupportedField = in.readOptionalBoolean();
        
        boost = in.readFloat();
        queryName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringArray(fields);
        
        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out);
        }
        out.writeVInt(ignoreItems.size());
        for (Item item : ignoreItems) {
            item.writeTo(out);
        }

        out.writeOptionalBoolean(include);
        out.writeOptionalString(minimumShouldMatch);
        out.writeVInt(minTermFreq);
        out.writeVInt(maxQueryTerms);
        out.writeOptionalStringArray(stopWords);
        out.writeVInt(minDocFreq);
        out.writeVInt(maxDocFreq);
        out.writeVInt(minWordLength);
        out.writeVInt(maxWordLength);
        out.writeFloat(boostTerms);
        out.writeOptionalString(analyzer);
        out.writeOptionalBoolean(failOnUnsupportedField);

        out.writeFloat(boost);
        out.writeOptionalString(queryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, items, ignoreItems, include, minimumShouldMatch, minTermFreq, maxQueryTerms, stopWords, minDocFreq,
                maxDocFreq, minWordLength, maxWordLength, boostTerms, analyzer, failOnUnsupportedField, boost, queryName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MoreLikeThisQueryBuilder other = (MoreLikeThisQueryBuilder) obj;
        return Objects.equals(fields, other.fields) && 
                Objects.equals(items, other.items) &&
                Objects.equals(ignoreItems, other.ignoreItems) &&
                Objects.equals(include, other.include) &&
                Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
                Objects.equals(minTermFreq, other.minTermFreq) &&
                Objects.equals(maxQueryTerms, other.maxQueryTerms) &&
                Objects.equals(stopWords, other.stopWords) &&
                Objects.equals(minDocFreq, other.minDocFreq) &&
                Objects.equals(maxDocFreq, other.maxDocFreq) &&
                Objects.equals(minWordLength, other.minWordLength) &&
                Objects.equals(maxWordLength, other.maxWordLength) &&
                Objects.equals(boostTerms, other.boostTerms) &&
                Objects.equals(analyzer, other.analyzer) &&
                Objects.equals(failOnUnsupportedField, other.failOnUnsupportedField) &&
                Objects.equals(boost, other.boost) &&
                Objects.equals(queryName, other.queryName);
    }
}
