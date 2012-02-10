package com.talis.entity.db.krati;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import krati.core.StoreConfig;
import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.DynamicDataStore;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;
import com.talis.entity.db.leveldb.LevelDbEntityDatabase;
import com.talis.entity.serializers.EntityDesc;
import com.talis.entity.serializers.POSerializer;

public class KratiEntityDatabase implements EntityDatabase {

	private static final Logger LOG = LoggerFactory.getLogger(KratiEntityDatabase.class);
	
	public static final String DB_LOCATION_PROPERTY = "com.talis.entity.store.krati.dir";
	public static final String DB_DEFAULT_LOCATION = "/mnt/entity-cache/krati";
	

	private static final int DEFAULT_KEY_COUNT = 20000000;
	private static final String SG_INDEX = "sg";
	private static final String GS_INDEX = "gs";
	public static final String DATA = "data";
	
	private final File dbDir;
	private final File dataDir;
	private final File gsDir;
	private final File sgDir;
	
	private final POSerializer serializer;
	
	private DataStore<byte[], byte[]> db;
	private DB sg;
	private DB gs;
	
	public KratiEntityDatabase (POSerializer serializer, String dbName){
		this.serializer = serializer;
		String rootPath = System.getProperty(DB_LOCATION_PROPERTY, DB_DEFAULT_LOCATION);
		File rootDir = new File(rootPath);
		dbDir = new File(rootDir, dbName);
		initDir(dbDir);
		LOG.info(String.format("Initialising DB at location : %s", dbDir.getAbsolutePath()));
		if (!dbDir.isDirectory()) {
			if (dbDir.exists()) {
				String msg = String.format("Invalid DB location: %s", dbDir.getAbsolutePath());
				LOG.error(msg);
				throw new RuntimeException(msg);
			}
		}
		sgDir = new File(dbDir, SG_INDEX);
		gsDir = new File(dbDir, GS_INDEX);
		dataDir = new File(dbDir, DATA);
		initDir(sgDir);
		initDir(gsDir);
		initDir(dataDir);
		
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
        	LOG.info("Initialising data store");
//        	StoreConfig config = new StoreConfig(dataDir, DEFAULT_KEY_COUNT);
//            config.setSegmentFactory(new ChannelSegmentFactory());
//            config.setSegmentFileSizeMB(128);
        	db = new DynamicDataStore(dataDir, 8, 10000, 5, 128, new ChannelSegmentFactory());
//                                   capacity, /* capacity */
//                                   10000,    /* update batch size */
//                                   5,        /* number of update batches required to sync indexes.dat */
//                                   128,      /* segment file size in MB */
//                                   createSegmentFactory());
        }catch(Exception e){
        	LOG.error("Unable to create datastore", e);
        	throw new RuntimeException("Error creating datastore", e);
        }
        
        initIndexes();
		LOG.info("Database initialised");
    }
	
	private void initIndexes(){
		LOG.info("Initialising indexes");
	    Options options = new Options();
		options.createIfMissing(true);
		options.compressionType(CompressionType.SNAPPY);
		try {
			gs = factory.open(gsDir, options);
			sg = factory.open(sgDir, options);
		} catch (IOException e) {
			LOG.error("Unable to provision TDB dataset", e);
			throw new RuntimeException("Error creating entity database", e);
		}
	}
    
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)
			throws EntityDatabaseException {
		LOG.debug("Storing entity bytes");
		byte[] storageKey = getStorageKey(subject, graph);
		try{
			db.put(storageKey, serializer.serialize(subject, graph, quads).bytes);
			sg.put(getSGKey(subject, graph), storageKey);
			gs.put(getGSKey(subject, graph), storageKey);
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
		try{
			db.delete(storageKey);
			sg.delete(getSGKey(subject, graph));
			gs.delete(getGSKey(subject, graph));
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
		DBIterator iterator = sg.iterator();
		EntityDesc desc = new EntityDesc();
		desc.subject = subject;
		try {
		  for(iterator.seek(key); iterator.hasNext(); iterator.next()) {
			String nextKey = asString(iterator.peekNext().getKey());
			String[] keyParts = new String(nextKey).split("\t");
			if (! subject.getURI().equals(keyParts[0])){
				continue;
			}
			desc.graph = Node.createURI(keyParts[1]);
			byte[] storageKey = iterator.peekNext().getValue();
			LOG.debug("Fetching single entity description");
			desc.bytes = db.get(storageKey);
			LOG.debug("Fetched single entity description");
			quads.addAll(serializer.deserialize(desc));
		  }
		} catch (IOException e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		} finally {
		  iterator.close();
		}
		LOG.debug("Combined entity descriptions");
		return quads;
	}

	public Collection<Quad> getGraph(Node graph) throws EntityDatabaseException {
		LOG.debug("Combining entity descriptions");
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(graph);
		DBIterator iterator = gs.iterator();
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		try {
		  for(iterator.seek(key); iterator.hasNext(); iterator.next()) {
			String nextKey = asString(iterator.peekNext().getKey());
			String[] keyParts = new String(nextKey).split("\t");
			if (! graph.getURI().equals(keyParts[0])){
				continue;
			}
			desc.subject = Node.createURI(keyParts[1]);
			byte[] storageKey = iterator.peekNext().getValue(); 
			desc.bytes = db.get(storageKey);
			quads.addAll(serializer.deserialize(desc));
		  }
		} catch (IOException e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		} finally {
		  iterator.close();
		}
		return quads;
	}
	
	public void clearGraph(Node graph) throws EntityDatabaseException {
		byte[] key = getKeyPrefix(graph);
		DBIterator iterator = gs.iterator();
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		WriteBatch gsBatch = gs.createWriteBatch();
		WriteBatch sgBatch = sg.createWriteBatch();
		try {
		  for(iterator.seek(key); iterator.hasNext(); iterator.next()) {
			byte[] nextKey = iterator.peekNext().getKey();
			String[] keyParts = asString(nextKey).split("\t");
			if (! graph.getURI().equals(keyParts[0])){
				continue;
			}
			String sg = new String(keyParts[1] + "\t" + keyParts[0]);
			byte[] storageKey = getStorageKeyAsBytes(sg);
			db.delete(storageKey);
			sgBatch.delete(sg.getBytes());
			gsBatch.delete(nextKey);
		  }
		  sg.write(sgBatch);
		  gs.write(gsBatch);
		} catch (Exception e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		} finally {
		  iterator.close();
		  gsBatch.close();
		  sgBatch.close();
		}
	}
	
	@Override
	public void clear() throws EntityDatabaseException {
		LOG.info("Clearing database");
		try {
			factory.destroy(sgDir, new Options());
			factory.destroy(gsDir, new Options());
			db.clear();
			LOG.debug("Datastore cleared, resetting indexes");
			initIndexes();
		} catch (Exception e) {
			LOG.error("Error clearing entity database", e);
			throw new EntityDatabaseException("Unable to clear entity database");
		}
		LOG.info("Cleared database");
	}

	@Override
	public void begin() throws EntityDatabaseException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() throws EntityDatabaseException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void abort() throws EntityDatabaseException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws EntityDatabaseException {
		try {
			db.close();
		} catch (IOException e) {
			LOG.error("Error closing datastore", e);
			throw new EntityDatabaseException("Error closing entity database", e);
		}
	}

	private byte[] getStorageKey(Node subject, Node graph){
		String s = getKeyString(subject, graph);
		return getStorageKeyAsBytes(s);
	}
	
	private byte[] getStorageKeyAsBytes(String s){
		String k = Integer.toString(s.hashCode(), 36); 
		return k.getBytes();
	}
	
	private String getKeyString(Node first, Node second){
		return first.getURI().concat("\t").concat(second.getURI());
	}
	
	private byte[] getKeyPrefix(Node node){
		return node.getURI().concat("\t").getBytes();
	}

	private byte[] getSGKey(Node subject, Node graph){
		return getKeyString(subject, graph).getBytes();
	}
	
	private byte[] getGSKey(Node subject, Node graph){
		return getKeyString(graph, subject).getBytes();
	}
	
	
	private String getUidFromString(String str){
		return Integer.toString(str.hashCode(), 36);
    }

}
