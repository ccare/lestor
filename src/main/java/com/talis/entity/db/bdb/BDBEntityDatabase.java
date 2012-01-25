package com.talis.entity.db.bdb;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.talis.entity.EntityDatabase;

public class BDBEntityDatabase implements EntityDatabase{

	private static final Logger LOG = LoggerFactory.getLogger(BDBEntityDatabase.class);
	
	public static final String BDB_ENCODING = "UTF-8";
	public static final String BDB_LOCATION_PROPERTY = "com.talis.entity.store.bdbDirectory";
	public static final String BDB_DEFAULT_LOCATION = "/tmp/entity-cache/bdb";
	public static final String DATABASE_NAME = "entity-cache";
	
	private Environment bdbEnvironment = null;		
	private Database bdbDatabase = null;
	private final Serializer serializer;
	
	public BDBEntityDatabase(String locationPath, Serializer serializer) throws IOException{
		LOG.info(String.format("Initialising BDB at location : %s", locationPath));
		File location = new File(locationPath);
		if (!location.isDirectory()) {
			if (location.exists()) {
				String msg = String.format("Invalid BDB location: %s", locationPath);
				LOG.error(msg);
				throw new IOException(msg);
			}
			LOG.info(String.format("Creating directory for BDB at location : %s", locationPath));
			if (location.mkdirs() != true) {
				String msg = String.format("Unable to create directory for BDB at location: %s", locationPath);
				LOG.error(msg);
				throw new IOException(msg);
			}
		}
		initialiseDatabase(location);
		this.serializer = serializer;
	}

	private void initialiseDatabase(File location) throws IOException {
		try {
		    EnvironmentConfig envConfig = new EnvironmentConfig();
		    envConfig.setAllowCreate(true);
		    envConfig.setSharedCache(true);
		    envConfig.setTransactional(false);
		    bdbEnvironment = new Environment(location, envConfig);
		} catch (Exception e) {
			String msg = String.format("Error initialising BDB environment.");
			LOG.error(msg);
			throw new IOException(msg, e);
		}	
		createDatabase();
	}
	
	private void createDatabase() throws IOException{
		try {
			LOG.debug("Creating database instance");
			String dbName = DATABASE_NAME;
			DatabaseConfig dbConfig = new DatabaseConfig();
	        dbConfig.setAllowCreate(true);
        	dbConfig.setTransactional(false);
        	
	        if (Boolean.getBoolean("deferred.write")){
	        	dbConfig.setDeferredWrite(true);
	        }		
	         
			bdbDatabase = bdbEnvironment.openDatabase(null, dbName, dbConfig);
			LOG.debug("Created database instance");
		} catch (Exception e) {
			String msg = String.format("Error initialising BDB database.");
			LOG.error(msg);
			throw new IOException(msg, e);
		}
	}
	
	public void close() throws IOException {
		try {
			bdbDatabase.close();
			bdbEnvironment.close();
		} catch (Exception e) {
			String msg = String.format("Error closing BDB database.");
			LOG.error(msg);
			throw new IOException(msg, e);
		}
	}
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads) throws IOException{
		LOG.debug("Storing entity bytes");
		byte[] key = getKey(subject, graph);
		DatabaseEntry dbKey = new DatabaseEntry(key);
		DatabaseEntry dbData = new DatabaseEntry(serializer.serialize(subject, graph, quads).bytes);
		OperationStatus status = null;
		try{
			status = bdbDatabase.put(null, dbKey, dbData);
		} catch (Exception e) {
			LOG.error("Unexpected exception writing to BDB", e);		
			throw new IOException("Unexpected exception writing to BDB", e);
		}
		if (status != OperationStatus.SUCCESS){
			LOG.error("Unexpected OperationStatus :" + status);
			throw new IOException("Unexpected OperationStatus :" + status);
		}
		LOG.debug("Stored entity bytes");
	}

	@Override
	public void delete(Node subject, Node graph) throws IOException {
		LOG.debug("Deleting entity bytes");
		byte[] key = getKey(subject, graph);
		DatabaseEntry dbKey = new DatabaseEntry(key);
		OperationStatus status = bdbDatabase.delete(null, dbKey); 
		if (status == OperationStatus.SUCCESS) {
			LOG.debug("Deleted entity bytes");
		}else{
			String message = "Error deleting data for key " + new String(key);
			LOG.error(message);
			LOG.error("OperationStatus: " + status);
			throw new IOException(message);
		}
	}
	
	@Override
	public Collection<Quad> get(Node subject) throws IOException {
		LOG.debug("Combining entity descriptions");
		byte[] key = getKeyPrefix(subject);
		DatabaseEntry dbKey = new DatabaseEntry(key);
		DatabaseEntry dbData = new DatabaseEntry();
		Cursor cursor = null;
		final Collection<Quad> quads = new HashSet<Quad>();
		try{
			cursor = bdbDatabase.openCursor(null, null);
			OperationStatus status= cursor.getSearchKeyRange(dbKey, dbData, LockMode.DEFAULT);
			EntityDesc desc = new EntityDesc();
			
			while( status == OperationStatus.SUCCESS) {
				LOG.debug("Opened cursor, returning iterable");
				String[] keyParts = new String(dbKey.getData()).split("\t");
				desc.subject = Node.createURI(keyParts[0]);
				desc.graph = Node.createURI(keyParts[1]);
				desc.bytes = dbData.getData();
				quads.addAll(serializer.deserialize(desc));
				status = cursor.getNext(dbKey, dbData, LockMode.DEFAULT);
			}
		}finally{
			cursor.close();
		}
		
		return quads;
	}
	
	@Override
	public void clear() throws IOException {
		LOG.debug("Clearing database");
		bdbDatabase.close();
		bdbEnvironment.removeDatabase(null, DATABASE_NAME);
		LOG.debug("Removed existing database");
		createDatabase();
		LOG.debug("Cleared database");
	}
	
	private byte[] getKeyPrefix(Node subject){
		return subject.getURI().concat("\t").getBytes();
	}
	
	private byte[] getKey(Node subject, Node graph){
		return subject.getURI().concat("\t").concat(graph.getURI()).getBytes();
	}

	@Override
	public void commit() {
		// do nothing
	}

}
