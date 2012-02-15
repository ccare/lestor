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

package com.talis.entity.db.tdb;

import org.apache.commons.io.FileUtils;
import org.junit.Before;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabasePerfTestBase;
import com.talis.platform.joon.VersionedDirectoryProvider;

public class TdbEntityDatabasePerfTest extends EntityDatabasePerfTestBase{

	DatasetProvider datasetProvider;
	
	@Before
	public void setup() throws Exception {
		datasetProvider = new DatasetProvider(
								new LocationProvider(
									new VersionedDirectoryProvider(tmpDir.getRoot())));
		FileUtils.cleanDirectory(tmpDir.getRoot());
		super.setup();
	}
	
	@Override
	public EntityDatabase getDatabase() {
		return new TdbEntityDatabase(id, datasetProvider);
	}

}
