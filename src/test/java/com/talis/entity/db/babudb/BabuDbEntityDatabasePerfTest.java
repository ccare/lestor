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
		dbManager = new DatabaseManager();
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
