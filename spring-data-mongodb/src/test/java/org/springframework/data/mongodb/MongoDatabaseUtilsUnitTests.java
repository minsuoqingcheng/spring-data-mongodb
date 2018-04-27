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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import javax.transaction.NotSupportedException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.session.ServerSession;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoDatabaseUtilsUnitTests {

	@Mock ClientSession session;
	@Mock ServerSession serverSession;
	@Mock MongoDbFactory dbFactory;
	@Mock MongoDatabase db;

	@Mock UserTransaction userTransaction;

	@Before
	public void setUp() {

		when(dbFactory.getSession(any())).thenReturn(session);

		when(dbFactory.withSession(session)).thenReturn(dbFactory);

		when(dbFactory.getDb()).thenReturn(db);

		when(session.getServerSession()).thenReturn(serverSession);
		when(session.hasActiveTransaction()).thenReturn(true);

		when(serverSession.isClosed()).thenReturn(false);
	}

	@After
	public void verifyTransactionSynchronizationManagerState() {

		assertTrue(TransactionSynchronizationManager.getResourceMap().isEmpty());
		assertFalse(TransactionSynchronizationManager.isSynchronizationActive());
		assertNull(TransactionSynchronizationManager.getCurrentTransactionName());
		assertFalse(TransactionSynchronizationManager.isCurrentTransactionReadOnly());
		assertNull(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
		assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
	}

	@Test // DATAMONGO-1920
	public void shouldNotStartSessionWhenNoTransactionOngoing() {

		MongoDatabaseUtils.getDatabase(dbFactory);

		verify(dbFactory, never()).getSession(any());
		verify(dbFactory, never()).withSession(any(ClientSession.class));
	}

	@Test // DATAMONGO-1920
	public void shouldParticipateInOngoingJtaTransactionWithCommit() throws SystemException, NotSupportedException {

		when(userTransaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION, Status.STATUS_ACTIVE,
				Status.STATUS_ACTIVE);

		JtaTransactionManager txManager = new JtaTransactionManager(userTransaction);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();

				MongoDatabaseUtils.getDatabase(dbFactory);

				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isTrue();
			}
		});

		verify(userTransaction).begin();

		verify(session).startTransaction();
		verify(session).commitTransaction();
		verify(session).close();
	}

	@Test // DATAMONGO-1920
	public void shouldParticipateInOngoingJtaTransactionWithRollback() throws SystemException, NotSupportedException {

		when(userTransaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION, Status.STATUS_ACTIVE,
				Status.STATUS_ACTIVE);

		JtaTransactionManager txManager = new JtaTransactionManager(userTransaction);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {

				assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isTrue();
				assertThat(transactionStatus.isNewTransaction()).isTrue();
				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isFalse();

				MongoDatabaseUtils.getDatabase(dbFactory);

				assertThat(TransactionSynchronizationManager.hasResource(dbFactory)).isTrue();

				transactionStatus.setRollbackOnly();
			}
		});

		verify(userTransaction).rollback();

		verify(session).startTransaction();
		verify(session).abortTransaction();
		verify(session).close();
	}

}
