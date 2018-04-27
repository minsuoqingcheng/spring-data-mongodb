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
package org.springframework.data.mongodb;

import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;

/**
 * Helper class for managing a {@link MongoDatabase} instances via {@link MongoDbFactory}. Used for obtaining
 * {@link ClientSession session bound} resources, such as {@link MongoDatabase} and
 * {@link com.mongodb.client.MongoCollection} suitable for transactional usage.
 * <p />
 * <strong>Note:</strong> Intended for internal usage only.
 *
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 * @since 2.1
 */
public class MongoDatabaseUtils {

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link MongoDbFactory factory}.
	 * <p />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the current
	 * {@link Thread} if {@link TransactionSynchronizationManager#isSynchronizationActive() snychronization is active}.
	 * 
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link MongoDbFactory} to get the {@link MongoDatabase} from.
	 * @return must not be {@literal null}.
	 */
	public static MongoDatabase getDatabase(String dbName, MongoDbFactory factory) {
		return doGetMongoDatabase(dbName, factory);
	}

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link MongoDbFactory factory}.
	 * <p />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the current
	 * {@link Thread} if {@link TransactionSynchronizationManager#isSynchronizationActive() snychronization is active}.
	 *
	 * @param factory the {@link MongoDbFactory} to get the {@link MongoDatabase} from.
	 * @return must not be {@literal null}.
	 */
	public static MongoDatabase getDatabase(MongoDbFactory factory) {
		return doGetMongoDatabase(null, factory);
	}

	private static MongoDatabase doGetMongoDatabase(@Nullable String dbName, MongoDbFactory factory) {

		Assert.notNull(factory, "Factory must not be null!");

		MongoDbFactory factoryToUse = TransactionSynchronizationManager.isSynchronizationActive()
				? factory.withSession(doGetSession(factory)) : factory;
		return StringUtils.hasText(dbName) ? factoryToUse.getDb(dbName) : factoryToUse.getDb();
	}

	private static ClientSession doGetSession(MongoDbFactory dbFactory) {

		MongoResourceHolder resourceHolder = (MongoResourceHolder) TransactionSynchronizationManager.getResource(dbFactory);

		if (resourceHolder != null && (resourceHolder.hasSession() || resourceHolder.isSynchronizedWithTransaction())) {

			resourceHolder.requested();
			if (!resourceHolder.hasSession()) {
				resourceHolder.setSession(createClientSession(dbFactory));
			}
			return resourceHolder.getSession();
		}

		resourceHolder = new MongoResourceHolder(createClientSession(dbFactory), dbFactory);
		resourceHolder.requested();
		resourceHolder.getSession().startTransaction();

		TransactionSynchronizationManager
				.registerSynchronization(new MongoSessionSynchronization(resourceHolder, dbFactory));
		resourceHolder.setSynchronizedWithTransaction(true);
		TransactionSynchronizationManager.bindResource(dbFactory, resourceHolder);

		return resourceHolder.getSession();
	}

	private static ClientSession createClientSession(MongoDbFactory dbFactory) {
		return dbFactory.getSession(ClientSessionOptions.builder().causallyConsistent(true).build());
	}

	/**
	 * MongoDB specific {@link ResourceHolderSynchronization} for resource cleanup at the end of a transaction when
	 * participating in a non-native MongoDB transaction, such as a Jta or JDBC transaction.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	private static class MongoSessionSynchronization extends ResourceHolderSynchronization<MongoResourceHolder, Object> {

		private final MongoResourceHolder resourceHolder;

		MongoSessionSynchronization(MongoResourceHolder resourceHolder, MongoDbFactory dbFactory) {

			super(resourceHolder, dbFactory);
			this.resourceHolder = resourceHolder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#shouldReleaseBeforeCompletion()
		 */
		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#processResourceAfterCommit(java.lang.Object)
		 */
		@Override
		protected void processResourceAfterCommit(MongoResourceHolder resourceHolder) {

			if (isTransactionActive(resourceHolder)) {
				resourceHolder.getSession().commitTransaction();
			}

		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#afterCompletion(int)
		 */
		@Override
		public void afterCompletion(int status) {

			if (status == TransactionSynchronization.STATUS_ROLLED_BACK && isTransactionActive(this.resourceHolder)) {
				resourceHolder.getSession().abortTransaction();
			}

			super.afterCompletion(status);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.ResourceHolderSynchronization#releaseResource(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected void releaseResource(MongoResourceHolder resourceHolder, Object resourceKey) {

			if (resourceHolder.hasActiveSession()) {
				resourceHolder.getSession().close();
			}
		}

		private boolean isTransactionActive(MongoResourceHolder resourceHolder) {

			if (!resourceHolder.hasSession()) {
				return false;
			}

			return resourceHolder.getSession().hasActiveTransaction();
		}

	}
}
