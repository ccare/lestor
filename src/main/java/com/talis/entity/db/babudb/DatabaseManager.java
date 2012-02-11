package com.talis.entity.db.babudb;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ConfigBuilder;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

import com.talis.entity.EntityDatabaseException;

public class DatabaseManager {
	
	private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);
	
	public static final String DB_LOCATION_PROPERTY = "com.talis.entity.store.babudb.dir";
	public static final String DB_DEFAULT_LOCATION = "/mnt/entity-cache/babudb";

	private final BabuDB dbSystem;
	
	public DatabaseManager(){
		LOG.info("Initialising DatabaseProvider");
		File rootDir = initRootDir();
		
		// use the builder to get a config with the default values
		BabuDBConfig config = new ConfigBuilder()
									.setCompressed(false)
									.setMultiThreaded(0)
									.setLogAppendSyncMode(SyncMode.ASYNC)
									.build();
		// now set our specific properties
		Properties props = config.getProps();
		props.setProperty("babudb.checkInterval", "30");
		props.setProperty("babudb.baseDir", rootDir.getAbsolutePath());
		props.setProperty("babudb.logDir", rootDir.getAbsolutePath() + "/logs");
		props.setProperty("babudb.maxLogfileSize", "" + FileUtils.ONE_MB * 64);
		
		try {
			BabuDBConfig conf = new BabuDBConfig(props);
			dbSystem = BabuDBFactory.createBabuDB(conf);
		} catch (Exception e) {
			LOG.error("Unable to initialise Database system", e);
			throw new RuntimeException("Error initialising Database system", e);
		}
	}
	

	private File initRootDir(){
		File dbDir = new File(System.getProperty(DB_LOCATION_PROPERTY, DB_DEFAULT_LOCATION));
       	LOG.info("Initialising Database root directory: {}", dbDir.getAbsolutePath());
   		if (!dbDir.isDirectory()) {
   			if (dbDir.exists()) {
   				String msg = String.format("Invalid Database root: {}", dbDir.getAbsolutePath());
   				LOG.error(msg);
   				throw new RuntimeException(msg);
   			}
   		}
   		return dbDir;
	}
	
	public Database getDatabase(String dbName) throws EntityDatabaseException{
		try{
    		return dbSystem.getDatabaseManager().getDatabase(dbName);
    	}catch(BabuDBException e){
    		if (e.getErrorCode().equals(BabuDBException.ErrorCode.NO_SUCH_DB)){
    			return createDatabase(dbName);
    		}else{
    			throw new EntityDatabaseException("Error creating entity database", e);
    		}
    	}
	}
	
	private Database createDatabase(String dbName) throws EntityDatabaseException{
		try{
			return dbSystem.getDatabaseManager().createDatabase(dbName, BabuDbEntityDatabase.NUM_INDEXES);
		}catch(Exception e){
    		throw new EntityDatabaseException("Error creating entity database", e);
    	}
	}
	
	public void deleteDatabase(String dbName) throws EntityDatabaseException{
		try {
			dbSystem.getDatabaseManager().deleteDatabase(dbName);
		} catch (BabuDBException e) {
			throw new EntityDatabaseException("Error deleting existing database", e);
		}
	}
	
	public void shutDown(){
		try {
			dbSystem.shutdown(true);
		} catch (BabuDBException e) {
			LOG.error("Error shutting down database manager", e);
		}
	}
}
