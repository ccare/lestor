package com.talis.entity.db.babudb;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;

public class BabuDBFactoryWrapper {

	public BabuDB createBabuDB(BabuDBConfig config) throws BabuDBException{
		return BabuDBFactory.createBabuDB(config);
	}
	
}
