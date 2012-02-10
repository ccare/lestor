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

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabasePerfTestBase;
import com.talis.entity.db.babudb.BabuDbEntityDatabase;
import com.talis.entity.db.babudb.DatabaseManager;
import com.talis.entity.serializers.POSerializer;
import com.talis.entity.serializers.SnappySerializer;

public class BabuDbEntityDatabasePerfTest extends EntityDatabasePerfTestBase{

	private DatabaseManager dbManager;
	
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
	public EntityDatabase getDatabase() {
		return new BabuDbEntityDatabase(new SnappySerializer(new POSerializer()), id, dbManager);
	}
}
