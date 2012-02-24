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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import com.talis.entity.EntityDatabase;
import com.talis.entity.compress.SnappyCodec;
import com.talis.entity.db.EntityDatabaseConcurrencyTestBase;
import com.talis.entity.marshal.Marshaller;

public class BabuDbEntityDatabaseConcurrencyTest extends EntityDatabaseConcurrencyTestBase{

	private File[] tmpDirs;
	int dbIndex = 0;

	@Before
	public void setup() throws Exception{
		tmpDirs = new File[NUM_DBS];
		for (int i=0;i<NUM_DBS;i++){
			File tmpDir = new File(FileUtils.getTempDirectory(), "db-con-test-" + i);
			FileUtils.forceMkdir(tmpDir);
			FileUtils.cleanDirectory(tmpDir);
			tmpDirs[i] = tmpDir;
		}
		super.setup();
	}
	
	@After
	public void tearDown() throws Exception{
		super.tearDown();
	}
	
	@Override
	public EntityDatabase getDatabase() throws Exception {
		DatabaseManager dbManager = new DatabaseManager(tmpDirs[dbIndex], new BabuDBFactoryWrapper());
		String thisId = id + "_" + dbIndex++;
		return new BabuDbEntityDatabase(new Marshaller(new SnappyCodec()), thisId, dbManager);
	}

}
