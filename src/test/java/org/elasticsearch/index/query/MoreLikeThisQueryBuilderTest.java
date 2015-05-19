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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.search.morelikethis.ItemSerializationTests;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

public class MoreLikeThisQueryBuilderTest extends BaseQueryTestCase<MoreLikeThisQueryBuilder> {

    @Override
    protected MoreLikeThisQueryBuilder createEmptyQueryBuilder() {
        return new MoreLikeThisQueryBuilder();
    }

    @Override
    protected MoreLikeThisQueryBuilder createTestQueryBuilder() {
        MoreLikeThisQueryBuilder query = new MoreLikeThisQueryBuilder(randomAsciiOfLength(8));
        query.like(ItemSerializationTests.generateRandomItem(5, 10));
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLength(8));
        }
        return query;
    }

    @Override
    // NO COMMIT: incomplete, we should rather test the query itself and not the builder (call toQuery)?
    protected void assertLuceneQuery(MoreLikeThisQueryBuilder queryBuilder, Query query, QueryParseContext context) throws IOException {
        assertThat(query, instanceOf(MoreLikeThisQuery.class));
        assertThat(query.getBoost(), is(queryBuilder.boost()));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) query;
        assertThat(mltQuery.getMoreLikeFields(), is(queryBuilder.fields()));
        
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo((Query)mltQuery));
        }
    }

    @Test
    public void testValidate() throws QueryParsingException, IOException {
        TermQueryBuilder queryBuilder = new TermQueryBuilder("all", "good");
        assertNull(queryBuilder.validate());

        queryBuilder = new TermQueryBuilder(null, "Term");
        assertNotNull(queryBuilder.validate());
        assertThat(queryBuilder.validate().validationErrors().size(), is(1));

        queryBuilder = new TermQueryBuilder("", "Term");
        assertNotNull(queryBuilder.validate());
        assertThat(queryBuilder.validate().validationErrors().size(), is(1));

        queryBuilder = new TermQueryBuilder("", null);
        assertNotNull(queryBuilder.validate());
        assertThat(queryBuilder.validate().validationErrors().size(), is(2));
    }
}
