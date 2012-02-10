package com.talis.entity.db.leveldb;

import java.util.ArrayList;
import java.util.Collection;

import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;
import com.talis.entity.serializers.EntityDesc;
import com.talis.entity.serializers.POSerializer;

public class LevelDbEntityDatabase implements EntityDatabase {

	private static final Logger LOG = LoggerFactory.getLogger(LevelDbEntityDatabase.class);
	
	public static final String DB_LOCATION_PROPERTY = "com.talis.entity.store.leveldb.dir";
	public static final String DB_DEFAULT_LOCATION = "/tmp/entity-cache/leveldb";
	public static final String DATABASE_NAME = "entity-cache";
	
	private final POSerializer serializer; 
	private final File dbDir; 
	private DB db;
	private DB secondary;
	
	public LevelDbEntityDatabase (POSerializer serializer, String dbName){
		this.serializer = serializer;
		String rootPath = System.getProperty(DB_LOCATION_PROPERTY, DB_DEFAULT_LOCATION);
		File rootDir = new File(rootPath);
		dbDir = new File(rootDir, dbName);
		LOG.info(String.format("Initialising LevelDB at location : %s", dbDir.getAbsolutePath()));
		if (!dbDir.isDirectory()) {
			if (dbDir.exists()) {
				String msg = String.format("Invalid LevelDB location: %s", dbDir.getAbsolutePath());
				LOG.error(msg);
				throw new RuntimeException(msg);
			}
			LOG.info(String.format("Creating directory for LevelDB at location : %s", dbDir.getAbsolutePath()));
			if (dbDir.mkdirs() != true) {
				String msg = String.format("Unable to create directory for LevelDB at location: %s", dbDir.getAbsolutePath());
				LOG.error(msg);
				throw new RuntimeException(msg);
			}
		}
		initDatabases();
	
	}
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)
			throws EntityDatabaseException {
		LOG.debug("Storing entity bytes");
		byte[] key = getKey(subject, graph);
		byte[] inverseKey = getKey(graph, subject);
		WriteOptions options = new WriteOptions().sync(false);
		try{
			byte[] value = serializer.serialize(subject, graph, quads).bytes;
			db.put(key, value, options);
			secondary.put(inverseKey, new byte[0], options);
		} catch (Exception e) {
			LOG.error("Unexpected exception writing to DB", e);
			throw new EntityDatabaseException("Unexpected exception writing to DB", e);
		}
		LOG.debug("Stored entity bytes");
	}

	@Override
	public void delete(Node subject, Node graph) throws EntityDatabaseException {
		LOG.debug("Deleting entity bytes");
		byte[] key = getKey(subject, graph);
		byte[] inverseKey = getKey(graph, subject);
		try{
			db.delete(key);
			secondary.delete(inverseKey);
		} catch (Exception e) {
			String message = "Error deleting data for key " + new String(key);
			LOG.error(message, e);
			throw new EntityDatabaseException(message);
		}
	}

	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		LOG.debug("Combining entity descriptions");
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(subject);
		String keyPrefix = subject.getURI() + "\t";
		DBIterator iterator = db.iterator();
		EntityDesc desc = new EntityDesc();
		desc.subject = subject;
		try {
		  for(iterator.seek(key); iterator.hasNext(); iterator.next()) {
			String nextKey = asString(iterator.peekNext().getKey());
			String[] keyParts = new String(nextKey).split("\t");
			if (! subject.getURI().equals(keyParts[0])){
				continue;
			}
			desc.subject = Node.createURI(keyParts[0]);
			desc.graph = Node.createURI(keyParts[1]);
			desc.bytes = iterator.peekNext().getValue();
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
	
	public Collection<Quad> getGraph(Node graph) throws EntityDatabaseException {
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(graph);
		DBIterator iterator = secondary.iterator();
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		try {
		  for(iterator.seek(key); iterator.hasNext(); iterator.next()) {
			String nextKey = asString(iterator.peekNext().getKey());
			String[] keyParts = new String(nextKey).split("\t");
			if (! graph.getURI().equals(keyParts[0])){
				continue;
			}
			byte[] primaryKey = new String(keyParts[1] + "\t" + keyParts[0]).getBytes(); 
			desc.subject = Node.createURI(keyParts[1]);
			desc.bytes = db.get(primaryKey);
			quads.addAll(serializer.deserialize(desc));
		  }
		} catch (Exception e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		} finally {
		  iterator.close();
		}
		return quads;
	}
	
	public void clearGraph(Node graph) throws EntityDatabaseException {
		byte[] key = getKeyPrefix(graph);
		DBIterator iterator = secondary.iterator();
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		WriteBatch batch = db.createWriteBatch();
		WriteBatch secondaryBatch = secondary.createWriteBatch();
		try {
		  for(iterator.seek(key); iterator.hasNext(); iterator.next()) {
			byte[] nextKey = iterator.peekNext().getKey();
			String[] keyParts = asString(nextKey).split("\t");
			if (! graph.getURI().equals(keyParts[0])){
				continue;
			}
			byte[] primaryKey = new String(keyParts[1] + "\t" + keyParts[0]).getBytes();
			batch.delete(primaryKey);
			secondaryBatch.delete(nextKey);
		  }
		  db.write(batch);
		  secondary.write(secondaryBatch);
		} catch (Exception e) {
			LOG.error("Unexpected exception reading from DB", e);		
			throw new EntityDatabaseException("Unexpected exception reading from DB", e);
		} finally {
		  iterator.close();
		  batch.close();
		  secondaryBatch.close();
		}
	}
	
	@Override
	public void clear() throws EntityDatabaseException {
		try {
			factory.destroy(dbDir, new Options());
			factory.destroy(new File(dbDir, "secondary"), new Options());
		} catch (Exception e) {
			LOG.error("Error clearing entity database", e);
			throw new EntityDatabaseException("Unable to clear entity database");
		}
		initDatabases();
	}

	private void initDatabases(){
		Options options = new Options();
		options.createIfMissing(true);
		options.compressionType(CompressionType.SNAPPY);
		try {
			db = factory.open(dbDir, options);
			secondary = factory.open(new File(dbDir, "secondary"), options);
		} catch (IOException e) {
			LOG.error("Unable to provision TDB dataset", e);
			throw new RuntimeException("Error creating entity database", e);
		}
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
		db.close();
	}
	
	private byte[] getKeyPrefix(Node subject){
		return subject.getURI().concat("\t").getBytes();
	}

	private byte[] getKey(Node subject, Node graph){
		return subject.getURI().concat("\t").concat(graph.getURI()).getBytes();
	}


}
