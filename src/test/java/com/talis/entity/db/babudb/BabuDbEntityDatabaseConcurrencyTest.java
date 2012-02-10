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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabaseConcurrencyTestBase;
import com.talis.entity.serializers.POSerializer;
import com.talis.entity.serializers.SnappySerializer;

public class BabuDbEntityDatabaseConcurrencyTest extends EntityDatabaseConcurrencyTestBase{

	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
	private DatabaseManager dbManager;
	
	int dbIndex = 0;

	@Before
	public void setup() throws Exception{
		System.setProperty(DatabaseManager.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		dbManager = new DatabaseManager(new BabuDBFactoryWrapper());
		super.setup();
	}
	
	@After
	public void tearDown() throws Exception{
		super.tearDown();
		System.clearProperty(DatabaseManager.DB_LOCATION_PROPERTY);
		dbManager.shutDown();
	}
	
	@Override
	public EntityDatabase getDatabase() throws Exception {
		String thisId = id + "_" + dbIndex++;
		return new BabuDbEntityDatabase(new SnappySerializer(new POSerializer()), thisId, dbManager);
	}

}
