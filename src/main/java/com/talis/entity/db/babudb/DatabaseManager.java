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

package com.talis.entity.db.babudb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ConfigBuilder;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.foundation.logging.Logging;

import com.talis.entity.EntityDatabaseException;

public class DatabaseManager {
	
	private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);
	
	public static final int CHECK_INTERVAL_DEFAULT = 30;
	public static final String CHECK_INTERVAL_PROPERTY = "com.talis.entity.store.babudb.checkInterval";
	
	public static final long MAX_LOG_SIZE_DEFAULT = 1024 * 1024 * 64;
	public static final String MAX_LOG_SIZE_PROPERTY = "com.talis.entity.store.babudb.maxLogfileSize";
	
	public static final String DEBUG_LOG_LEVEL_PROPERTY = "com.talis.entity.store.babudb.debugLogLevel";
	public static final String DEBUG_LOG_FILE_PROPERTY = "com.talis.entity.store.babudb.debugLogFile";
	
	private final BabuDB dbSystem;
	
	public DatabaseManager(File dbDir, BabuDBFactoryWrapper babuDBFactory){
		LOG.info("Initialising DatabaseProvider");
		initDbDir(dbDir);
		
		// use the builder to get a config with the default values
		BabuDBConfig config = new ConfigBuilder()
									.setCompressed(false)
									.setMultiThreaded(0)
									.setLogAppendSyncMode(SyncMode.ASYNC)
									.build();
		// now set our specific properties
		Properties props = config.getProps();
		props.setProperty("babudb.baseDir", dbDir.getAbsolutePath());
		props.setProperty("babudb.logDir", dbDir.getAbsolutePath() + "/logs");
		props.setProperty("babudb.checkInterval", System.getProperty(CHECK_INTERVAL_PROPERTY, "" + CHECK_INTERVAL_DEFAULT));
		props.setProperty("babudb.maxLogfileSize", System.getProperty(MAX_LOG_SIZE_PROPERTY, "" + MAX_LOG_SIZE_DEFAULT));
		props.setProperty("babudb.debug.level", System.getProperty(DEBUG_LOG_LEVEL_PROPERTY, "" + getDebugLogLevel()));
		
		if (null != System.getProperty(DEBUG_LOG_FILE_PROPERTY)){
			LOG.info("Redirecting BabuDB debug logs to " + System.getProperty(DEBUG_LOG_FILE_PROPERTY));
			File debugLog = new File(System.getProperty(DEBUG_LOG_FILE_PROPERTY));
			try {
				Logging.redirect(new PrintStream(new FileOutputStream(debugLog)));
			} catch (FileNotFoundException e) {
				LOG.error("Unable to redirect BabuDB debug log", e);
			}
		}
		
		try {
			BabuDBConfig conf = new BabuDBConfig(props);
			dbSystem = babuDBFactory.createBabuDB(conf);
		} catch (Exception e) {
			LOG.error("Unable to initialise Database system", e);
			throw new RuntimeException("Error initialising Database system", e);
		}
	}
	
	public final int getDebugLogLevel(){
		String logLevel = System.getProperty(DEBUG_LOG_LEVEL_PROPERTY); 
		if ("EMERGENCY".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_EMERG;
		}else if ("ALERT".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_ALERT;
		}else if ("CRITICAL".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_CRIT;
		}else if ("ERROR".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_ERROR;
		}else if ("WARN".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_WARN;
		}else if ("NOTICE".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_NOTICE;
		}else if ("INFO".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_INFO;
		}else if ("DEBUG".equalsIgnoreCase(logLevel)){
			return Logging.LEVEL_DEBUG;
		}else{
			return Logging.LEVEL_WARN;
		}
	}
	

	private void initDbDir(File dbDir){
       	LOG.info("Initialising Database directory: {}", dbDir.getAbsolutePath());
		try{
       		FileUtils.forceMkdir(dbDir);
		} catch (IOException e) {
			LOG.error("Error creating Database directory", e);
			throw new RuntimeException("Unable to create database", e);
		}
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
			LOG.info("Shutting down");
			dbSystem.shutdown(true);
		} catch (Exception e) {
			LOG.error("Error shutting down database manager", e);
		}
	}
	
}
