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
