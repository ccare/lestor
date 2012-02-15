package com.talis.entity.db.babudb;

import org.junit.After;
import org.junit.Before;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabaseTestBase;
import com.talis.entity.serializers.POSerializer;
import com.talis.entity.serializers.SnappySerializer;

public class BabuDbEntityDatabaseTest extends EntityDatabaseTestBase {

	private DatabaseManager dbManager;
	
	@Before
	public void setup() throws Exception{
		System.setProperty(DatabaseManager.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		dbManager = new DatabaseManager(new BabuDBFactoryWrapper());
		super.setup();
	}
	
	@After
	public void tearDown() throws Exception{
		System.clearProperty(DatabaseManager.DB_LOCATION_PROPERTY);
		db.close();
		dbManager.shutDown();
	}
	
	@Override
	public EntityDatabase getDatabase() {
		return new BabuDbEntityDatabase(new SnappySerializer(new POSerializer()), id, dbManager);
	}
	
}
