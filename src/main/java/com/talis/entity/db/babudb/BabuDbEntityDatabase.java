package com.talis.entity.db.babudb;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.tools.DBDumpTool;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;
import com.talis.entity.serializers.EntityDesc;
import com.talis.entity.serializers.Serializer;
import com.talis.platform.joon.VersionedDirectoryProvider;
import com.talis.platform.joon.initialisation.DirectoryInitialiser;
import com.talis.platform.joon.initialisation.RollbackUpdateException;

public class BabuDbEntityDatabase implements EntityDatabase{

	private static final Logger LOG = LoggerFactory.getLogger(BabuDbEntityDatabase.class);
	
	public static final String DB_LOCATION_PROPERTY = "com.talis.entity.store.babudb.dir";
	public static final String DB_DEFAULT_LOCATION = "/mnt/entity-cache/babudb";
	
	private static final int SUBJECT_INDEX = 0;
	private static final int GRAPH_INDEX = 1;
	private static final int NUM_INDEXES = 2;
	
	public static final String DATA = "data";
	private final String dbName;
	private File dbDir;
	private File dataDir;
	
	private final Serializer serializer;
	private final VersionedDirectoryProvider directoryProvider;
	
	private BabuDB dbSystem;
	private Database db;

	public BabuDbEntityDatabase(Serializer serializer, String dbName, VersionedDirectoryProvider directoryProvider){
		this.directoryProvider = directoryProvider;
		this.serializer = serializer;
		this.dbName = dbName;
		initDatabase();
	}

	private void initDir(File dir){
		LOG.info(String.format("Creating directory for DB at location : %s", dbDir.getAbsolutePath()));
		try{
			FileUtils.forceMkdir(dir);
		}catch (Exception e){
			String msg = String.format("Unable to create directory for DB at location: %s", dir.getAbsolutePath());
			LOG.error(msg, e);
			throw new RuntimeException(msg, e);
		}
	}
	
	protected void initDatabase(){
        try{
        	dbDir = directoryProvider.getLiveDirectory(dbName);
        	LOG.info(String.format("Initialising DB at location : %s", dbDir.getAbsolutePath()));
    		if (!dbDir.isDirectory()) {
    			if (dbDir.exists()) {
    				String msg = String.format("Invalid DB location: %s", dbDir.getAbsolutePath());
    				LOG.error(msg);
    				throw new RuntimeException(msg);
    			}
    		}
    		dataDir = new File(dbDir, DATA);
    		initDir(dataDir);
        	
        	LOG.info("Initialising data store");
        	String baseDir = dbDir.getAbsolutePath();
        	String logDir = new File(dbDir, "logs").getAbsolutePath();
        	int numThreads = 0;
        	long maxLogFileSize = FileUtils.ONE_MB * 16;
        	int checkInterval = 20;
        	SyncMode syncMode = SyncMode.ASYNC;
        	int pseudoSyncWait = 0;
        	int maxQ = 0;
        	boolean compression = false;
        	int maxBlockSize = 64;
        	int maxNumRecordsPerBlock = 52428800;
        	boolean disableMMap = false;
        	int mmapLimit = -1;
        	int debugLevel = 4;
        	
        	BabuDBConfig config = new BabuDBConfig(baseDir, logDir, numThreads, maxLogFileSize,
        											checkInterval, syncMode, pseudoSyncWait, 
        											maxQ, compression, maxNumRecordsPerBlock, maxBlockSize,
        											disableMMap, mmapLimit, debugLevel); 
        	dbSystem = BabuDBFactory.createBabuDB(config);
//        			new ConfigBuilder()
//        				.setDataPath(dataDir.getAbsolutePath())
//        				.setCompressed(false)
//        				.setMultiThreaded(0)
//        				.setLogAppendSyncMode(SyncMode.ASYNC)
//        				.build());
        	try{
        		db = dbSystem.getDatabaseManager().getDatabase("data");
        	}catch(BabuDBException e){
        		if (e.getErrorCode().equals(BabuDBException.ErrorCode.NO_SUCH_DB)){
        			db = dbSystem.getDatabaseManager().createDatabase("data", NUM_INDEXES);
        		}else{
        			throw e;
        		}
        	}
        }catch(Exception e){
        	LOG.error("Unable to create datastore", e);
        	throw new RuntimeException("Error creating datastore", e);
        }
        
		LOG.info("Database initialised");
    }
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)
			throws EntityDatabaseException {
		LOG.debug("Storing entity bytes");
		byte[] storageKey = getStorageKey(subject, graph);
		byte[] inverseKey = getInverseKey(subject, graph);
		try{
			DatabaseInsertGroup batch = db.createInsertGroup();
			batch.addInsert(0, storageKey, serializer.serialize(subject, graph, quads).bytes);
			batch.addInsert(1, inverseKey, storageKey);
			db.insert(batch, null);
		} catch (Exception e) {
			LOG.error("Unexpected exception writing to DB", e);
			throw new EntityDatabaseException("Unexpected exception writing to DB", e);
		}
		LOG.debug("Stored entity bytes");		
	}

	@Override
	public void delete(Node subject, Node graph) throws EntityDatabaseException {
		LOG.debug("Deleted entity bytes");
		byte[] storageKey = getStorageKey(subject, graph);
		byte[] inverseKey = getInverseKey(subject, graph);
		try{
			DatabaseInsertGroup batch = db.createInsertGroup();
			batch.addInsert(0, storageKey, null);
			batch.addInsert(1, inverseKey, null);
			db.insert(batch, null);
		} catch (Exception e) {
			LOG.error("Unexpected exception writing to DB", e);
			throw new EntityDatabaseException("Unexpected exception writing to DB", e);
		}
		LOG.debug("Deleted entity bytes");
	}

	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		LOG.debug("Combining entity descriptions");
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(subject);
		EntityDesc desc = new EntityDesc();
		desc.subject = subject;
		try {
			Iterator<Entry<byte[], byte[]>> iterator = db.prefixLookup(0, key, null).get();
			while(iterator.hasNext()) {
			    Entry<byte[], byte[]> pair = iterator.next();
			    String nextKey = asString(pair.getKey());
			    String[] keyParts = new String(nextKey).split("\t");
			    if (! subject.getURI().equals(keyParts[0])){
			    	continue;
			    }
				desc.graph = Node.createURI(keyParts[1]);
				LOG.debug("Fetching single entity description");
				desc.bytes = pair.getValue();
				LOG.debug("Fetched single entity description");
				quads.addAll(serializer.deserialize(desc));
		  }
		} catch (Exception e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		} 
		LOG.debug("Combined entity descriptions");
		return quads;
	}

	@Override
	public void clear() throws EntityDatabaseException {
		File currentLiveDirectory = null;
    	LOG.debug("Getting active directory and updating to next version");
    	try{
			currentLiveDirectory = directoryProvider.getDereferencedLiveDirectory(dbName);
			LOG.info("Initialising new database directory");
			
			directoryProvider.updateVersion(dbName, new DirectoryInitialiser(){
				@Override
				public void build(File newDirectoryLocation)
						throws RollbackUpdateException {
					// nothing to do
				}
			});
		}catch(IOException e){
			LOG.warn("Error switching live location directory", e);
			throw new EntityDatabaseException("Error switching live db directory", e);
		}
		    	
		// a) do this async at some point
		// b) hopefully, no-one is trying to access it right now - should be ok, databases
		// are only accessed by a single thread AT THE MOMENT
		LOG.debug("Deleting previous location {}", currentLiveDirectory.getAbsolutePath());
		
		if (null != currentLiveDirectory){
			LOG.info("Removing previous directory {}", currentLiveDirectory.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(currentLiveDirectory);
			} catch (IOException e) {
				LOG.warn("Unable to clean up old Dataset directory", e);
			}
		}
		initDatabase();
	}
	
	public Collection<Quad> getGraph(Node graph) throws EntityDatabaseException {
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(graph);
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		try {
		  Iterator<Entry<byte[], byte[]>> iterator = db.prefixLookup(1, key, null).get();
		  while (iterator.hasNext()){
			  Entry<byte[], byte[]> pair = iterator.next();
			  String nextKey = asString(pair.getKey());
			  String[] keyParts = new String(nextKey).split("\t");
			  if (! graph.getURI().equals(keyParts[0])){
			    	continue;
			  }
			  desc.subject = Node.createURI(keyParts[1]);
			  LOG.debug("Fetching single entity description");
			  desc.bytes = db.lookup(0, pair.getValue(), null).get();
			  LOG.debug("Fetched single entity description");
			  quads.addAll(serializer.deserialize(desc));
		  }
		} catch (Exception e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		}
		return quads;
	}
	
	public void clearGraph(Node graph) throws EntityDatabaseException {
		byte[] key = getKeyPrefix(graph);
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		DatabaseInsertGroup batch = db.createInsertGroup();
		try {
		  Iterator<Entry<byte[], byte[]>> iterator = db.prefixLookup(1, key, null).get();
		  while (iterator.hasNext()){
			  Entry<byte[], byte[]> pair = iterator.next();
			  String nextKey = asString(pair.getKey());
			  String[] keyParts = new String(nextKey).split("\t");
			  if (! graph.getURI().equals(keyParts[0])){
			    	continue;
			  }
			  desc.subject = Node.createURI(keyParts[1]);
			  batch.addDelete(0, pair.getValue());
			  batch.addDelete(1, pair.getKey());
		  }
		  db.insert(batch, null);
		} catch (Exception e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		}
	}
	

	@Override
	public void begin() throws EntityDatabaseException {
	}

	@Override
	public void commit() throws EntityDatabaseException {
	}

	@Override
	public void abort() throws EntityDatabaseException {
	}

	@Override
	public void close() throws EntityDatabaseException {
		try {
			db.shutdown();
		} catch (BabuDBException e) {
			LOG.error("Error closing entity database", e);
			throw new EntityDatabaseException("Error closing entity database", e);
		}
	}

	private byte[] getStorageKey(Node subject, Node graph){
		return getKeyString(subject, graph).getBytes();
	}
	
	private byte[] getInverseKey(Node subject, Node graph){
		return getKeyString(graph, subject).getBytes();
	}
	
	private String getKeyString(Node first, Node second){
		return first.getURI().concat("\t").concat(second.getURI());
	}
	
	private byte[] getKeyPrefix(Node node){
		return node.getURI().concat("\t").getBytes();
	}
	
	public static String asString(byte value[]) {
		if( value == null) {
			return null;
		}
		try {
			return new String(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public void dump(){
		DBDumpTool.RecordFormatter formatter = new DBDumpTool.RecordFormatter(){
			
			@Override
			public String formatKey(byte[] arg0, String arg1, int arg2) {
				return new String(arg0);
			}

			@Override
			public String formatValue(byte[] arg0, byte[] arg1, String arg2, int arg3) {
				return "";
			}
			
		};
		try {
			DBDumpTool.dumpDB(dbDir.getAbsolutePath(), 
							  dbDir.getAbsolutePath() + "/logs",
							  false, formatter, System.out);
		} catch (BabuDBException e) {
			
			e.printStackTrace();
		}
	}

	
}
