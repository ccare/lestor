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
