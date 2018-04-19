/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.test.util.MongoCollectionTestUtils.*;

import java.util.ArrayList;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * @author Christoph Strobl
 * @since 2018/04
 */
public class TxTests {

	@Test
	public void commitTx() {

		try (MongoClient client = new MongoClient("localhost",
				MongoClientOptions.builder().readPreference(ReadPreference.primary()).build())) {

			MongoCollection<Document> collection = createOrReplaceCollection("client-session-tx-tests", "tx-tests", client);

			ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());

			Bson filter = Filters.eq("_id", "1");

			session.startTransaction();
			collection.insertOne(session, new Document("_id", "1"));
			session.commitTransaction();

			session.close();

			assertThat(collection.find(filter).limit(1).into(new ArrayList())).containsExactly(new Document("_id", "1"));
		}
	}
}
