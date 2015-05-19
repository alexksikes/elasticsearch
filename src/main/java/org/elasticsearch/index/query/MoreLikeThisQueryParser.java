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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MoreLikeThisQueryParser extends BaseQueryParser {

    public static final String NAME = "mlt";

    public static class Fields {
        public static final ParseField LIKE_TEXT = new ParseField("like_text").withAllDeprecated("like");
        public static final ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
        public static final ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MAX_WORD_LENGTH = new ParseField("max_word_length", "max_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
        public static final ParseField BOOST_TERMS = new ParseField("boost_terms");
        public static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
        public static final ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");
        public static final ParseField STOP_WORDS = new ParseField("stop_words");
        public static final ParseField DOCUMENT_IDS = new ParseField("ids").withAllDeprecated("like");
        public static final ParseField DOCUMENTS = new ParseField("docs").withAllDeprecated("like");
        public static final ParseField LIKE = new ParseField("like");
        public static final ParseField IGNORE_LIKE = new ParseField("ignore_like");
        public static final ParseField INCLUDE = new ParseField("include");
    }

    @Inject
    public MoreLikeThisQueryParser() {

    }

    @Override
    public String[] names() {
        return new String[]{NAME, "more_like_this", "moreLikeThis"};
    }

    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MoreLikeThisQueryBuilder mltQuery = new MoreLikeThisQueryBuilder();

        XContentParser.Token token;
        String currentFieldName = null;

        List<Item> items = new ArrayList<>();
        List<Item> ignoreItems = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.LIKE_TEXT.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, items);
                } else if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, items);
                } else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, ignoreItems);
                } else if (Fields.MIN_TERM_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.minTermFreq(parser.intValue());
                } else if (Fields.MAX_QUERY_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.maxQueryTerms(parser.intValue());
                } else if (Fields.MIN_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.minDocFreq(parser.intValue());
                } else if (Fields.MAX_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.maxDocFreq(parser.intValue());
                } else if (Fields.MIN_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.minWordLength(parser.intValue());
                } else if (Fields.MAX_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.maxWordLength(parser.intValue());
                } else if (Fields.BOOST_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    float boostFactor = parser.floatValue();
                    if (boostFactor > 0) {
                        mltQuery.boostTerms(boostFactor);
                    }
                } else if (Fields.MINIMUM_SHOULD_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.minimumShouldMatch(parser.text());
                } else if ("analyzer".equals(currentFieldName)) {
                    mltQuery.analyzer(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.boost(parser.floatValue());
                } else if (Fields.FAIL_ON_UNSUPPORTED_FIELD.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.failOnUnsupportedField(parser.booleanValue());
                } else if ("_name".equals(currentFieldName)) {
                    mltQuery.queryName(parser.text());
                } else if (Fields.INCLUDE.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.include(parser.booleanValue());
                } else {
                    throw new QueryParsingException(parseContext, "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.STOP_WORDS.match(currentFieldName, parseContext.parseFlags())) {
                    List<String> stopWords = new ArrayList<>();
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.stopWords(stopWords.toArray(Strings.EMPTY_ARRAY));
                } else if ("fields".equals(currentFieldName)) {
                    List<String> fields = new ArrayList<>();
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parseContext.indexName(parser.text()));
                    }
                    mltQuery.fields(fields.toArray(Strings.EMPTY_ARRAY));
                } else if (Fields.DOCUMENT_IDS.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (!token.isValue()) {
                            throw new IllegalArgumentException("ids array element should only contain ids");
                        }
                        items.add((Item) new Item().id(parser.text()));
                    }
                } else if (Fields.DOCUMENTS.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new IllegalArgumentException("docs array element should include an object");
                        }
                        items.add(parseItem(parser));
                    }
                } else if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, items);
                    }
                } else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, ignoreItems);
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, items);
                }
                else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, ignoreItems);
                } else {
                    throw new QueryParsingException(parseContext, "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        // finally set the items
        if (!items.isEmpty()) {
            mltQuery.like(items.toArray(Item.EMPTY_ARRAY));
        }
        if (!ignoreItems.isEmpty()) {
            mltQuery.ignoreLike(ignoreItems.toArray(Item.EMPTY_ARRAY));
        }

        mltQuery.validate();
        return mltQuery;
    }

    private void parseLikeField(XContentParser parser, List<Item> items) throws IOException {
        if (parser.currentToken().isValue()) {
            items.add(new Item(parser.text()));
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            items.add(parseItem(parser));
        } else {
            throw new IllegalArgumentException("Content of 'like' parameter should either be a string or an object");
        }
    }

    private Item parseItem(XContentParser parser) throws IOException {
        Item item = new Item();
        item.parseRequest(item, parser);
        return item;
    }
}
