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

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.termvectors.vectorize.Vectorizer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@Slow
public class TermVectorsVectorizeTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleVectorizer() throws ExecutionException, InterruptedException, IOException {
        logger.info("setup the index");
        Settings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "whitespace");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "name", "type=string", "quote", "type=string"));
        ensureYellow();

        logger.info("index the documents");
        indexRandom(true, client().prepareIndex("test", "type1", "0").setSource(jsonBuilder()
                        .startObject()
                        .field("name", "Spiderman")
                        .field("quote", "That’s what I love about this city. Every time I need to hit someone really, " +
                                "really hard, some jerk steps up and volunteers.")
                        .endObject()),
                client().prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                        .startObject()
                        .field("name", "Thor")
                        .field("quote", "I have taken thy measure, villain, and found it lacking.")
                        .endObject()),
                client().prepareIndex("test", "type1", "2").setSource(jsonBuilder()
                        .startObject()
                        .field("name", "Wolverine")
                        .field("quote", "Only two knives, Elektra? I got six.")
                        .endObject()));

        logger.info("building the vectorizer");
        List<Term> terms = new ArrayList<>();
        terms.add(new Term("name", "spiderman"));
        terms.add(new Term("name", "wolverine"));
        for (String s : new String[]{"i", "someone", "villain", "lacking", "elektra", "six", "volunteers", "found no where"}) {
            terms.add(new Term("quote", s));
        }
        Map<String, Vectorizer.ValueOption> valueOptions = new HashMap<>();
        valueOptions.put("name", Vectorizer.ValueOption.TERM_FREQ);
        valueOptions.put("quote", Vectorizer.ValueOption.DOC_FREQ);
        Vectorizer vectorizer = new Vectorizer(terms, valueOptions);
        
        logger.info("comparing the vectors returned of document 0");
        Vectorizer.SparseVector vector = client().prepareTermVectors("test", "type1", "0")
                    .setSelectedFields("name", "quote")
                    .setTermStatistics(true)
                    .setDfs(true)
                    .setVectorizer(vectorizer)
                    .get().getVector();

        assertEquals(vector.getShape(), terms.size());
        assertEquals(vector.getMaxSize(), 4);
        Tuple<int[], double[]> indicesAndValues = vector.getIndicesAndValues();
        assertArrayEquals(indicesAndValues.v1(), new int[]{0, 2, 3, 8});
        assertArrayEquals(indicesAndValues.v2(), new double[]{1, 3, 1, 1}, 0);

        logger.info("comparing the vectors returned of document 1");
        vector = client().prepareTermVectors("test", "type1", "1")
                .setSelectedFields("name", "quote")
                .setTermStatistics(true)
                .setDfs(true)
                .setVectorizer(vectorizer)
                .get().getVector();

        assertEquals(vector.getShape(), terms.size());
        assertEquals(vector.getMaxSize(), 3);
        indicesAndValues = vector.getIndicesAndValues();
        assertArrayEquals(indicesAndValues.v1(), new int[]{2, 4, 5});
        assertArrayEquals(indicesAndValues.v2(), new double[]{3, 1, 1}, 0);

        logger.info("comparing the vectors returned of document 2");
        vector = client().prepareTermVectors("test", "type1", "2")
                .setSelectedFields("name", "quote")
                .setTermStatistics(true)
                .setDfs(true)
                .setVectorizer(vectorizer)
                .get().getVector();

        assertEquals(vector.getShape(), terms.size());
        assertEquals(vector.getMaxSize(), 4);
        indicesAndValues = vector.getIndicesAndValues();
        assertArrayEquals(indicesAndValues.v1(), new int[]{1, 2, 6, 7});
        assertArrayEquals(indicesAndValues.v2(), new double[]{1, 3, 1, 1}, 0);
    }

    @Test
    public void testRandomVectorizer() throws ExecutionException, InterruptedException, IOException {
        logger.info("create the expected random doc-term matrix");
        int numRows = scaledRandomIntBetween(5, 25);  // number of documents
        int numCols = scaledRandomIntBetween(5, 25); // number of features
        List<Triple<Integer, String, Integer>>[] matrix = new List[numRows];  // column index, feature name, feature value
        for (int i = 0; i < numRows; i++) {
            matrix[i] = new ArrayList<>();
            for (int j = 0; j < numCols; j++) {
                if (randomBoolean()) {
                    matrix[i].add(new ImmutableTriple<>(j, "feature_" + j, randomIntBetween(1, 5)));
                }
            }
        }

        logger.info("setup the index");
        Settings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "whitespace");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "field", "type=string"));  // we assume only one field
        ensureYellow();

        logger.info("index the documents");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < matrix.length; i++) {
            String text = "";
            for (Triple<Integer, String, Integer> entry : matrix[i]) {
                // repeat for term frequencies
                for (int k = 0; k < entry.getRight(); k++) {
                    text += " " + entry.getMiddle();
                }
            }
            builders.add(client().prepareIndex("test", "type1", i + "").setSource("field", text));
        }
        indexRandom(true, builders);

        logger.info("building the vectorizer");
        Map<String, Vectorizer.ValueOption> valueOptions = new HashMap<>();
        valueOptions.put("field", Vectorizer.ValueOption.TERM_FREQ);  // only consider term freq
        List<Term> terms = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                terms.add(new Term("field", "feature_" + j));
            }
        }
        Vectorizer vectorizer = new Vectorizer(terms, valueOptions);

        logger.info("comparing the vectors returned");
        for (int i = 0; i < numRows; i++) {
            TermVectorsResponse response = client().prepareTermVectors("test", "type1", i + "")
                    .setSelectedFields("field")
                    .setTermStatistics(true)
                    .setVectorizer(vectorizer)
                    .get();
            compareVectors(response.getVector(), matrix[i], numCols);
        }
    }

    private void compareVectors(Vectorizer.SparseVector vector, List<Triple<Integer, String, Integer>> entries, int shape) throws IOException {
        assertEquals(vector.getShape(), shape);
        assertEquals(vector.getMaxSize(), entries.size());
        Tuple<int[], double[]> indicesAndValues = vector.getIndicesAndValues();
        int j = 0;
        for (Triple<Integer, String, Integer> entry : entries) {
            assertEquals((int) entry.getLeft(), indicesAndValues.v1()[j]);
            assertEquals((double) entry.getRight(), indicesAndValues.v2()[j++], 0);
        }
    }
}
