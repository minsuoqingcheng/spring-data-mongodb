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
package org.springframework.data.mongodb.repository;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.test.util.MongoCollectionTestUtils.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.test.util.AfterTransactionAssertion;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.data.util.Version;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Transactional(transactionManager = "txManager")
public class PersonRepositoryTransactionalTests {

	public static @ClassRule RuleChain TEST_RULES = RuleChain.outerRule(MongoVersionRule.atLeast(Version.parse("3.7.3")))
			.around(ReplicaSet.required());

	static final String DB_NAME = "repository-tx-tests";

	@Configuration
	@EnableMongoRepositories
	static class Config extends AbstractMongoConfiguration {

		@Bean
		public MongoClient mongoClient() {
			return new MongoClient();
		}

		@Override
		protected String getDatabaseName() {
			return DB_NAME;
		}

		@Bean
		MongoTransactionManager txManager(MongoDbFactory dbFactory) {
			return new MongoTransactionManager(dbFactory);
		}
	}

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Autowired MongoClient client;
	@Autowired PersonRepository repository;
	@Autowired MongoTemplate template;

	Person durzo, kylar, vi;

	List<Person> all;

	List<org.springframework.data.mongodb.test.util.AfterTransactionAssertion<Persistable<String>>> assertionList;

	@Before
	public void setUp() throws InterruptedException {
		assertionList = new CopyOnWriteArrayList<>();
	}

	@BeforeTransaction
	public void beforeTransaction() throws InterruptedException {

		createOrReplaceCollection(DB_NAME, template.getCollectionName(Person.class), client);

		durzo = new Person("Durzo", "Blint", 700);
		kylar = new Person("Kylar", "Stern", 21);
		vi = new Person("Viridiana", "Sovari", 20);

		all = repository.saveAll(Arrays.asList(durzo, kylar, vi));
	}

	@AfterTransaction
	public void verifyDbState() throws InterruptedException {

		MongoCollection<Document> collection = client.getDatabase(DB_NAME)
				.getCollection(template.getCollectionName(Person.class));

		assertionList.forEach(it -> {

			boolean isPresent = collection.find(Filters.eq("_id", new ObjectId(it.getId().toString()))).limit(1).iterator()
					.hasNext();

			assertThat(isPresent).isEqualTo(it.shouldBePresent())
					.withFailMessage(String.format("After transaction entity %s should %s.", it.getPersistable(),
							it.shouldBePresent() ? "be present" : "NOT be present"));
		});
	}

	@Rollback(false)
	@Test // DATAMONGO-1920
	public void shouldHonorCommitForDerivedQuery() {

		repository.removePersonByLastnameUsingAnnotatedQuery(durzo.getLastname());

		assertAfterTransaction(durzo).isNotPresent();
	}

	@Rollback(false)
	@Test // DATAMONGO-1920
	public void shouldHonorCommit() {

		Person hu = new Person("Hu", "Gibbet", 43);

		repository.save(hu);

		assertAfterTransaction(hu).isPresent();
	}

	@Test // DATAMONGO-1920
	public void shouldHonorRollback() {

		Person hu = new Person("Hu", "Gibbet", 43);

		repository.save(hu);

		assertAfterTransaction(hu).isNotPresent();
	}

	private AfterTransactionAssertion assertAfterTransaction(Person person) {

		AfterTransactionAssertion assertion = new AfterTransactionAssertion(new Persistable() {

			@Nullable
			@Override
			public Object getId() {
				return person.id;
			}

			@Override
			public boolean isNew() {
				return person.id != null;
			}
		});

		assertionList.add(assertion);
		return assertion;
	}

}
