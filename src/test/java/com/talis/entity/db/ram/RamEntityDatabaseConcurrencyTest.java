package com.talis.entity.db.ram;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabaseConcurrencyTestBase;

public class RamEntityDatabaseConcurrencyTest extends EntityDatabaseConcurrencyTestBase{

	@Override
	public EntityDatabase getDatabase() {
		return new RamEntityDatabase();
	}

}
