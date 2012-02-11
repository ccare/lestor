package com.talis.entity.db.ram;

import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabasePerfTestBase;
import com.talis.entity.db.ram.RamEntityDatabase;

public class RamEntityDatbasePerfTest extends EntityDatabasePerfTestBase{

	@Override
	public EntityDatabase getDatabase() {
		return new RamEntityDatabase();
	}

}
