/*
 *    Copyright 2012 Talis Systems Ltd
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.talis.entity.db.babudb;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.Checkpointer;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ConfigBuilder;

import com.talis.entity.EntityDatabaseException;

public class DatabaseManagerTest {

	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
	private BabuDBConfig defaultConfig;
	
	@Before
	public void setup(){
		System.setProperty(DatabaseManager.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		defaultConfig = new ConfigBuilder()
								.setDataPath(tmpDir.getRoot().getAbsolutePath(), 
											tmpDir.getRoot().getAbsolutePath() + "/logs")
								.build();
	}
	
	@After
	public void teardown(){
		System.clearProperty(DatabaseManager.DB_LOCATION_PROPERTY);
	}
	
	@Test
	public void getDatabaseCreatesIfNecessary() throws Exception{
		final BabuDB dbSystem = BabuDBFactory.createBabuDB(defaultConfig);
		BabuDBFactoryWrapper factory = new BabuDBFactoryWrapper(){
			@Override
			public BabuDB createBabuDB(BabuDBConfig config) throws BabuDBException {
				return dbSystem;
			};
		};
		DatabaseManager manager = new DatabaseManager(factory);
		assertTrue(dbSystem.getDatabaseManager().getDatabases().isEmpty());
		manager.getDatabase("test-db");
		assertFalse(dbSystem.getDatabaseManager().getDatabases().isEmpty());
		assertNotNull(dbSystem.getDatabaseManager().getDatabase("test-db"));
	}
	
	@Test
	public void getDatabaseReturnsExistingDb() throws Exception{
		BabuDB dbSystem = BabuDBFactory.createBabuDB(defaultConfig);
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		dbSystem.getDatabaseManager().createDatabase("test-db", 2);
		Database db = dbSystem.getDatabaseManager().getDatabase("test-db");
		assertSame(db, manager.getDatabase("test-db"));
	}
	
	@Test
	public void deleteExistingDatabase() throws Exception{
		BabuDB dbSystem = BabuDBFactory.createBabuDB(defaultConfig);
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		dbSystem.getDatabaseManager().createDatabase("test-db", BabuDbEntityDatabase.NUM_INDEXES);
		assertTrue(dbSystem.getDatabaseManager().getDatabases().keySet().contains("test-db"));
		manager.deleteDatabase("test-db");
		assertFalse(dbSystem.getDatabaseManager().getDatabases().keySet().contains("test-db"));
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void attemptToDeleteNonExistentDatabase() throws Exception{
		BabuDB dbSystem = BabuDBFactory.createBabuDB(defaultConfig);
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		assertFalse(dbSystem.getDatabaseManager().getDatabases().keySet().contains("test-db"));
		manager.deleteDatabase("test-db");
		assertFalse(dbSystem.getDatabaseManager().getDatabases().keySet().contains("test-db"));
	 	
	}
	
	@Test
	public void shutdownCheckpointsAndDelegatesToDbSystem() throws Exception{
		Checkpointer checkpointer = createStrictMock(Checkpointer.class);
		checkpointer.checkpoint();
		checkpointer.waitForCheckpoint();
		replay(checkpointer);
		
		final BabuDB dbSystem = createStrictMock(BabuDB.class);
		dbSystem.getCheckpointer();
		expectLastCall().andReturn(checkpointer).anyTimes();
		dbSystem.shutdown(true);
		replay(dbSystem);
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		try{
			manager.shutDown();
		}finally{
			verify(checkpointer);
			verify(dbSystem);
		}
	}
	
	@Test 
	public void dbSystemInitialisedWithCustomConfiguration() throws Exception{
		ObservableFactory factory = new ObservableFactory();
		new DatabaseManager(factory);
		assertNotNull(factory.suppliedConfig);
		assertEquals(DatabaseManager.CHECK_INTERVAL_DEFAULT, factory.suppliedConfig.getCheckInterval());
		assertEquals(DatabaseManager.MAX_LOG_SIZE_DEFAULT, factory.suppliedConfig.getMaxLogfileSize());
		assertEquals(tmpDir.getRoot().getAbsolutePath() + "/", factory.suppliedConfig.getBaseDir());
	}
	
	@Test 
	public void overrideConfigSettingsWithSystemProperties() throws Exception{
		System.setProperty(DatabaseManager.CHECK_INTERVAL_PROPERTY, "99");
		System.setProperty(DatabaseManager.MAX_LOG_SIZE_PROPERTY, "99999");
		
		ObservableFactory factory = new ObservableFactory();
		new DatabaseManager(factory);
		assertNotNull(factory.suppliedConfig);
		assertEquals(99, factory.suppliedConfig.getCheckInterval());
		assertEquals(99999, factory.suppliedConfig.getMaxLogfileSize());
	}
	
	@Test (expected=RuntimeException.class) 
	public void throwRuntimeExceptionIfDbRootDirMissing() throws Exception{
		File bogus = new File("/this/does/not/exist");
		assertFalse(bogus.exists());
		System.setProperty(DatabaseManager.DB_LOCATION_PROPERTY, bogus.getAbsolutePath());
		new DatabaseManager(new BabuDBFactoryWrapper());
	}
	
	@Test (expected=RuntimeException.class) 
	public void throwRuntimeExceptionIfDbRootNotDirectory() throws Exception{
		File bogus = tmpDir.newFile("bogus");
		bogus.createNewFile();
		assertTrue(bogus.exists());
		assertFalse(bogus.isDirectory());
		System.setProperty(DatabaseManager.DB_LOCATION_PROPERTY, bogus.getAbsolutePath());
		new DatabaseManager(new BabuDBFactoryWrapper());
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void handleExceptionOpeningDatabase() throws Exception{
		BabuDBException expectedException = new BabuDBException(ErrorCode.INTERNAL_ERROR, "BANG!");
		org.xtreemfs.babudb.api.DatabaseManager dbManager = 
				createStrictMock(org.xtreemfs.babudb.api.DatabaseManager.class);
		dbManager.getDatabase("test-db");
		expectLastCall().andThrow(expectedException);
		replay(dbManager);

		final BabuDB dbSystem = createStrictMock(BabuDB.class);
		dbSystem.getDatabaseManager();
		expectLastCall().andReturn(dbManager);
		replay(dbSystem);
		
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		try{
			manager.getDatabase("test-db");
		}catch(Exception e){
			assertSame(expectedException, e.getCause());
			throw e;
		}finally{
			verify(dbManager);
			verify(dbSystem);
		}
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void handleExceptionCreatingDatabase() throws Exception{
		BabuDBException expectedException = new BabuDBException(ErrorCode.INTERNAL_ERROR, "BANG!");
		org.xtreemfs.babudb.api.DatabaseManager dbManager = 
				createStrictMock(org.xtreemfs.babudb.api.DatabaseManager.class);
		dbManager.getDatabase("test-db");
		expectLastCall().andThrow(new BabuDBException(ErrorCode.NO_SUCH_DB, "not found"));
		dbManager.createDatabase("test-db", BabuDbEntityDatabase.NUM_INDEXES);
		expectLastCall().andThrow(expectedException);
		replay(dbManager);

		final BabuDB dbSystem = createStrictMock(BabuDB.class);
		dbSystem.getDatabaseManager();
		expectLastCall().andReturn(dbManager).anyTimes();
		replay(dbSystem);
		
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		try{
			manager.getDatabase("test-db");
		}catch(Exception e){
			assertSame(expectedException, e.getCause());
			throw e;
		}finally{
			verify(dbManager);
			verify(dbSystem);
		}
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void handleExceptionDeletingDatabase() throws Exception{
		BabuDBException expectedException = new BabuDBException(ErrorCode.INTERNAL_ERROR, "BANG!");
		org.xtreemfs.babudb.api.DatabaseManager dbManager = 
				createStrictMock(org.xtreemfs.babudb.api.DatabaseManager.class);
		dbManager.deleteDatabase("test-db");
		expectLastCall().andThrow(expectedException);
		replay(dbManager);

		final BabuDB dbSystem = createStrictMock(BabuDB.class);
		dbSystem.getDatabaseManager();
		expectLastCall().andReturn(dbManager).anyTimes();
		replay(dbSystem);
		
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		try{
			manager.deleteDatabase("test-db");
		}catch(Exception e){
			assertSame(expectedException, e.getCause());
			throw e;
		}finally{
			verify(dbManager);
			verify(dbSystem);
		}
	}
	
	@Test
	public void exceptionShuttingDownDbSystemIsSwallowed() throws Exception{
		BabuDBException expectedException = new BabuDBException(ErrorCode.INTERNAL_ERROR, "BANG!");
		
		Checkpointer checkpointer = createStrictMock(Checkpointer.class);
		checkpointer.checkpoint();
		checkpointer.waitForCheckpoint();
		replay(checkpointer);
		
		final BabuDB dbSystem = createStrictMock(BabuDB.class);
		dbSystem.getCheckpointer();
		expectLastCall().andReturn(checkpointer).anyTimes();
		dbSystem.shutdown(true);
		expectLastCall().andThrow(expectedException);
		replay(dbSystem);
		
		DatabaseManager manager = new DatabaseManager(getWrapperForDbSystem(dbSystem));
		try{
			manager.shutDown();
		}finally{
			verify(dbSystem);
		}		
	}

	private BabuDBFactoryWrapper getWrapperForDbSystem(final BabuDB dbSystem){
		BabuDBFactoryWrapper factory = new BabuDBFactoryWrapper(){
			@Override
			public BabuDB createBabuDB(BabuDBConfig config) throws BabuDBException {
				return dbSystem;
			};
		};
		return factory;
	}

	class ObservableFactory extends BabuDBFactoryWrapper{
		BabuDBConfig suppliedConfig;
		@Override
		public BabuDB createBabuDB(BabuDBConfig config) throws BabuDBException {
			suppliedConfig = config;
			return super.createBabuDB(config);
		}
	}

	
}
