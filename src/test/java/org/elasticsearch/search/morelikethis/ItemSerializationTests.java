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

package org.elasticsearch.search.morelikethis;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.is;

public class ItemSerializationTests extends ElasticsearchTestCase {

    public static Item generateRandomItem(int fieldsSize, int stringSize) {
        String index = randomAsciiOfLength(stringSize);
        String type = randomAsciiOfLength(stringSize);
        String id = String.valueOf(Math.abs(randomInt()));
        String routing = randomBoolean() ? randomAsciiOfLength(stringSize) : null;
        String[] fields = generateRandomStringArray(fieldsSize, stringSize, true);

        long version = Math.abs(randomLong());
        VersionType versionType = RandomPicks.randomFrom(new Random(), VersionType.values());

        return (Item) new Item(index, type, id).routing(routing).selectedFields(fields).version(version).versionType(versionType);
    }

    private String ItemToJSON(Item item) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startArray("docs");
        item.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endArray();
        builder.endObject();
        return XContentHelper.convertToJson(builder.bytes(), false);
    }

    private Item JSONtoItem(String json) throws Exception {
        BytesReference data = new BytesArray(json);
        Item item = new Item();
        TermVectorsRequest.parseRequest(item, XContentFactory.xContent(data).createParser(data));
        return item;
    }

    @Test
    public void testItemSerialization() throws Exception {
        int numOfTrials = 100;
        int maxArraySize = 7;
        int maxStringSize = 8;
        for (int i = 0; i < numOfTrials; i++) {
            Item item1 = generateRandomItem(maxArraySize, maxStringSize);
            String json = ItemToJSON(item1);
            Item item2 = JSONtoItem(json);
            assertEquals(item1, item2);
        }
    }

    private List<Item> testItemsFromJSON(String json) throws Exception {
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new Item(), new BytesArray(json));
        List<Item> items = (List<Item>) request.subRequests();

        assertEquals(items.size(), 3);
        for (Item item : items) {
            assertThat(item.index(), is("test"));
            assertThat(item.type(), is("type"));
            switch (item.id()) {
                case "1" :
                    assertThat(item.selectedFields().toArray(Strings.EMPTY_ARRAY), is(new String[]{"field1"}));
                    break;
                case "2" :
                    assertThat(item.selectedFields().toArray(Strings.EMPTY_ARRAY), is(new String[]{"field2"}));
                    break;
                case "3" :
                    assertThat(item.selectedFields().toArray(Strings.EMPTY_ARRAY), is(new String[]{"field3"}));
                    break;
                default:
                    fail("item with id: " + item.id() + " is not 1, 2 or 3");
                    break;
            }
        }
        return items;
    }

    @Test
    public void testSimpleItemSerializationFromFile() throws Exception {
        // test items from JSON
        List<Item> itemsFromJSON = testItemsFromJSON(
                copyToStringFromClasspath("/org/elasticsearch/search/morelikethis/items.json"));

        // create builder from items
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startArray("docs");
        for (Item item : itemsFromJSON) {
            Item itemForBuilder = (Item) new Item(
                    item.index(), item.type(), item.id()).selectedFields(item.selectedFields().toArray(Strings.EMPTY_ARRAY));
            itemForBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
        builder.endObject();

        // verify generated JSON lead to the same items
        String json = XContentHelper.convertToJson(builder.bytes(), false);
        testItemsFromJSON(json);
    }

}
