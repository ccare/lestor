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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabaseConcurrencyTestBase;
import com.talis.platform.joon.VersionedDirectoryProvider;

public class TdbEntityDatabaseConcurrencyTest extends EntityDatabaseConcurrencyTestBase{

	@Rule
	public TemporaryFolder tmpDir0 = new TemporaryFolder();
	
	@Rule
	public TemporaryFolder tmpDir1 = new TemporaryFolder();
	
	@Rule
	public TemporaryFolder tmpDir2 = new TemporaryFolder();
	
	@Rule
	public TemporaryFolder tmpDir3 = new TemporaryFolder();
	
	@Rule
	public TemporaryFolder tmpDir4 = new TemporaryFolder();
	
	private int dirIndex = 0;
	private File[] dirs; 
	
	@Before
	public void setup() throws Exception{
		dirs = new File[]{
				tmpDir0.getRoot(),
				tmpDir1.getRoot(),
				tmpDir2.getRoot(),
				tmpDir3.getRoot(),
				tmpDir4.getRoot()
		};
		
		super.setup();
	}
	
	@Override
	public EntityDatabase getDatabase() throws Exception {
		File tmpDir = dirs[dirIndex++];
		FileUtils.cleanDirectory(tmpDir);
		DatasetProvider datasetProvider = new DatasetProvider(
											new LocationProvider(
												new VersionedDirectoryProvider(tmpDir)));
		return new TdbEntityDatabase(id, datasetProvider);
	}

}
