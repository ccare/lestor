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

import static com.talis.entity.db.babudb.Keys.asString;
import static com.talis.entity.db.babudb.Keys.getInverseKey;
import static com.talis.entity.db.babudb.Keys.getKeyPrefix;
import static com.talis.entity.db.babudb.Keys.getStorageKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.exception.BabuDBException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;
import com.talis.entity.EntityDesc;
import com.talis.entity.marshal.Marshaller;

public class BabuDbEntityDatabase implements EntityDatabase{

	private static final String DB_WRITE_ERROR_MESSAGE = "Unexpected exception writing to DB";
	private static final String DB_READ_ERROR_MESSAGE = "Unexpected exception reading from DB";

	private static final Logger LOG = LoggerFactory.getLogger(BabuDbEntityDatabase.class);
	
	private static final int SUBJECT_INDEX = 0;
	private static final int GRAPH_INDEX = 1;
	public static final int NUM_INDEXES = 2;
	
	private final String dbName;
	private final Marshaller marshaller;
	private final DatabaseManager dbManager;
	private Database db;

	public BabuDbEntityDatabase(Marshaller marshaller, String dbName, DatabaseManager dbManager){
		this.marshaller = marshaller;
		this.dbName = dbName;
		this.dbManager = dbManager;
		try {
			db = dbManager.getDatabase(dbName);
		} catch (EntityDatabaseException e) {
			throw new RuntimeException("Unable to initialise database", e);
		}
	}
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)
			throws EntityDatabaseException {
		LOG.debug("Storing entity bytes");
		byte[] storageKey = getStorageKey(subject, graph);
		byte[] inverseKey = getInverseKey(subject, graph);
		try{
			DatabaseInsertGroup batch = db.createInsertGroup();
			batch.addInsert(SUBJECT_INDEX, storageKey, marshaller.toEntityDesc(subject, graph, quads).bytes);
			batch.addInsert(GRAPH_INDEX, inverseKey, storageKey);
			db.insert(batch, null);
		} catch (Exception e) {
			LOG.error(DB_WRITE_ERROR_MESSAGE, e);
			throw new EntityDatabaseException(DB_WRITE_ERROR_MESSAGE, e);
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
			batch.addInsert(SUBJECT_INDEX, storageKey, null);
			batch.addInsert(GRAPH_INDEX, inverseKey, null);
			db.insert(batch, null);
		} catch (Exception e) {
			LOG.error(DB_WRITE_ERROR_MESSAGE, e);
			throw new EntityDatabaseException(DB_WRITE_ERROR_MESSAGE, e);
		}
		LOG.debug("Deleted entity bytes");
	}

	@Override
	public boolean exists(Node subject) throws EntityDatabaseException {
		LOG.debug("Checking for existence of {}", subject.getURI());
		byte[] key = getKeyPrefix(subject);
		try {
			Iterator<Entry<byte[], byte[]>> iterator = 
						db.prefixLookup(SUBJECT_INDEX, key, null).get();
			LOG.debug("Finished subject lookup for {}", subject.getURI());
			return iterator.hasNext();
		} catch (BabuDBException e) {
			LOG.error("Error performing subject lookup", e);
			throw new EntityDatabaseException("Unable to lookup subject", e);
		}
	}
	
	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		LOG.debug("Combining entity descriptions");
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(subject);
		EntityDesc desc = new EntityDesc();
		desc.subject = subject;
		try {
			Iterator<Entry<byte[], byte[]>> iterator = db.prefixLookup(SUBJECT_INDEX, key, null).get();
			while(iterator.hasNext()) {
			    Entry<byte[], byte[]> pair = iterator.next();
			    String[] keyParts = asString(pair.getKey()).split("\t");
			    if (! subject.getURI().equals(keyParts[0])){
			    	continue;
			    }
				desc.graph = Node.createURI(keyParts[1]);
				LOG.debug("Fetching single entity description");
				desc.bytes = pair.getValue();
				LOG.debug("Fetched single entity description");
				quads.addAll(marshaller.toQuads(desc));
		  }
		} catch (Exception e) {
			LOG.error(DB_READ_ERROR_MESSAGE, e);		
			throw new EntityDatabaseException(DB_READ_ERROR_MESSAGE, e);
		} 
		LOG.debug("Combined entity descriptions");
		return quads;
	}

	public Collection<Quad> getGraph(Node graph) throws EntityDatabaseException {
		ArrayList<Quad> quads = new ArrayList<Quad>();
		byte[] key = getKeyPrefix(graph);
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		try {
		  Iterator<Entry<byte[], byte[]>> iterator = db.prefixLookup(GRAPH_INDEX, key, null).get();
		  while (iterator.hasNext()){
			  Entry<byte[], byte[]> pair = iterator.next();
			  String[] keyParts = asString(pair.getKey()).split("\t");
			  if (! graph.getURI().equals(keyParts[0])){
			    	continue;
			  }
			  desc.subject = Node.createURI(keyParts[1]);
			  LOG.debug("Fetching single entity description");
			  desc.bytes = db.lookup(SUBJECT_INDEX, pair.getValue(), null).get();
			  LOG.debug("Fetched single entity description");
			  quads.addAll(marshaller.toQuads(desc));
		  }
		} catch (Exception e) {
			LOG.error(DB_READ_ERROR_MESSAGE, e);		
			throw new EntityDatabaseException(DB_READ_ERROR_MESSAGE, e);
		}
		return quads;
	}
	
	@Override
	public void deleteGraph(Node graph) throws EntityDatabaseException {
		byte[] key = getKeyPrefix(graph);
		EntityDesc desc = new EntityDesc();
		desc.graph = graph;
		DatabaseInsertGroup batch = db.createInsertGroup();
		try {
		  Iterator<Entry<byte[], byte[]>> iterator = db.prefixLookup(GRAPH_INDEX, key, null).get();
		  while (iterator.hasNext()){
			  Entry<byte[], byte[]> pair = iterator.next();
			  String[] keyParts = asString(pair.getKey()).split("\t");
			  if (! graph.getURI().equals(keyParts[0])){
			    	continue;
			  }
			  desc.subject = Node.createURI(keyParts[1]);
			  batch.addDelete(SUBJECT_INDEX, pair.getValue());
			  batch.addDelete(GRAPH_INDEX, pair.getKey());
		  }
		  db.insert(batch, null);
		} catch (Exception e) {
			LOG.error(DB_READ_ERROR_MESSAGE, e);		
			throw new EntityDatabaseException(DB_READ_ERROR_MESSAGE, e);
		}
	}
	
	@Override
	public Iterable<Entry<Node, Iterable<Quad>>> all() throws EntityDatabaseException {
		LOG.info("Creating Iterable over entire database");
		try {
			return new EntityIterable();
		} catch (BabuDBException e) {
			LOG.error("Error creating db iterator", e);
			throw new EntityDatabaseException("Unable to create entity iterator", e);
		}
	}
	
	@Override
	public void clear() throws EntityDatabaseException {
    	LOG.info("Clearing entity database");
    	try{
    		db.shutdown();
    		LOG.debug("Database shutdown");
    		dbManager.deleteDatabase(dbName);
    		LOG.debug("Database deleted");
    		db = dbManager.getDatabase(dbName);
    		LOG.debug("Database recreated");
		}catch(Exception e){
			LOG.warn("Error clearing entity database", e);
			throw new EntityDatabaseException("Error clearing entity database", e);
		}
    	LOG.info("Cleared entity database");
	}
	
	@Override
	public void close() throws EntityDatabaseException {
		try {
			db.shutdown();
			dbManager.shutDown();
		} catch (BabuDBException e) {
			LOG.error("Error closing entity database", e);
			throw new EntityDatabaseException("Error closing entity database", e);
		}
	}

	@Override
	public void begin() throws EntityDatabaseException { /* noop */ }

	@Override
	public void commit() throws EntityDatabaseException { /* noop */ }

	@Override
	public void abort() throws EntityDatabaseException { /* noop */ }
	
	
	private class EntityIterable implements Iterable<Entry<Node, Iterable<Quad>>>{
		
		private final Iterator<Entry<byte[], byte[]>> dbIterator;
		
		public EntityIterable() throws BabuDBException{
			dbIterator = db.prefixLookup(SUBJECT_INDEX, null, null).get();
		}
		
		@Override
		public Iterator<Entry<Node, Iterable<Quad>>> iterator() {
			return new Iterator<Entry<Node, Iterable<Quad>>>(){
				private String currentSubject = null;
				{
					// when we create the iterator, move it to the first item
					if (dbIterator.hasNext()){
						Entry<byte[], byte[]> pair = dbIterator.next();
						String[] keyParts = asString(pair.getKey()).split("\t");
						currentSubject = keyParts[0];
					}
				}
								
				@Override
				public boolean hasNext() {
					return null != currentSubject;
				}
	
				@Override
				public Entry<Node, Iterable<Quad>> next() {
					// build an Entry to return from the current subject
					try{
						Node subject = Node.createURI(currentSubject);
						Entity entity = new Entity(subject, get(subject));
						
						// move the iterator on to the next subject
						if (!dbIterator.hasNext()){
							// nothing more in the db
							currentSubject = null;
						}
						
						while (dbIterator.hasNext()){
							Entry<byte[], byte[]> pair = dbIterator.next();
							String[] keyParts = asString(pair.getKey()).split("\t");
							if (! keyParts[0].equals(currentSubject)){
								currentSubject = keyParts[0];
								break;
							}
						}
						
						return entity;
					} catch (EntityDatabaseException e) {
						LOG.error("Error during iteration", e);
						throw new RuntimeException("Error retrieving quads for subject " + currentSubject, e);
					}
				}
	
				@Override
				public void remove() {
					throw new UnsupportedOperationException("Not supported");					
				}
			};
		
		}
	}
	
	static class Entity implements Entry<Node, Iterable<Quad>>{

		private final Node subject;
		private final Iterable<Quad> quads;
		
		Entity(Node subject, Iterable<Quad> quads){
			this.subject = subject;
			this.quads = quads;
		}
		
		@Override
		public Node getKey() {
			return subject;
		}

		@Override
		public Iterable<Quad> getValue() {
			return quads;
		}

		@Override
		public Iterable<Quad> setValue(Iterable<Quad> value) {
			throw new UnsupportedOperationException("Not Supported");
		}
		
	}
}
