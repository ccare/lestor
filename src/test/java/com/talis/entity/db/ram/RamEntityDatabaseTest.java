package com.talis.entity.db.ram;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabaseTestBase;


public class RamEntityDatabaseTest extends EntityDatabaseTestBase{

	@Override
	public EntityDatabase getDatabase() {
		return new RamEntityDatabase();
	}
	
}
