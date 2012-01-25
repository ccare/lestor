package com.talis.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.db.bdb.BDBEntityDatabase;
import com.talis.entity.db.bdb.GzipSerializer;
import com.talis.entity.db.bdb.NQuadsSerializer;
import com.talis.entity.db.bdb.NTriplesSerializer;
import com.talis.entity.db.bdb.Serializer;
import com.talis.entity.db.ram.RAMEntityDatabase;
import com.talis.entity.db.tdb.LocationProvider;
import com.talis.entity.db.tdb.TDBEntityDatabase;
import com.talis.entity.db.tdb.TdbDatasetProvider;
import com.talis.platform.joon.VersionedDirectoryProvider;

public class EntityManagerTest {

	Node subject;
	Node graph;
	Collection<Quad> quads;
	File dbDirectory;
	String id;
	
	@Before
	public void setup() throws Exception {
		subject = Node.createURI("http://example.com/s");
		graph = Node.createURI("http://example.com/g");
		quads = getQuads(graph, subject, 30);
		dbDirectory = new File("/tmp/entity-cache/db/" + UUID.randomUUID().toString());
		id = "test-id";
		initDBDirectory();
	}
	
	@After
	public void tearDown() throws IOException{
		FileUtils.forceDelete(dbDirectory);
	}
	
	@Test
	public void roundTripStatementsInRAMStore() throws Exception{
		EntityDatabase em = new RAMEntityDatabase();
		roundTripStatements(em);
	}
	
	@Test
	public void roundTripStatementsInBDBStore() throws Exception{
		Serializer serializer = new NQuadsSerializer();
		BDBEntityDatabase em = new BDBEntityDatabase(dbDirectory.getAbsolutePath(), serializer);
		roundTripStatements(em);
		em.close();
	}
	
	@Test
	public void roundTripStatementsInTDBStore() throws Exception{
		EntityDatabase em = new TDBEntityDatabase(id, new VersionedDirectoryProvider(dbDirectory));
		roundTripStatements(em);
	}
	
	@Test
	public void clearRAMStore() throws Exception {
		EntityDatabase em = new RAMEntityDatabase();
		checkClearingDatabase(em);
	}
	
	@Test
	public void clearBDBStore() throws Exception {
		Serializer serializer = new NQuadsSerializer();
		BDBEntityDatabase em = new BDBEntityDatabase(dbDirectory.getAbsolutePath(), serializer);
		checkClearingDatabase(em);
	}
	
	@Test
	public void clearTDBStore() throws Exception {
		EntityDatabase em = new TDBEntityDatabase(id, new VersionedDirectoryProvider(dbDirectory));
		checkClearingDatabase(em);
	}
	
	@Test
	public void multipleDescriptionsFromRAMStore() throws Exception{
		EntityDatabase em = new RAMEntityDatabase();
		combineMultipleDescriptions(em);
	}

	@Test
	public void multipleDescriptionsFromBDBStore() throws Exception{
		Serializer serializer = new NQuadsSerializer();
		BDBEntityDatabase em = new BDBEntityDatabase(dbDirectory.getAbsolutePath(), serializer);
		combineMultipleDescriptions(em);
		em.close();
	}
		
	@Test
	public void multipleDescriptionsFromTDBStore() throws Exception{
		EntityDatabase em = new TDBEntityDatabase(id, new VersionedDirectoryProvider(dbDirectory));
		combineMultipleDescriptions(em);
	}
		
	@Test
	public void benchmarkRAMStore() throws Exception {
		EntityDatabase em = new RAMEntityDatabase();
		benchmarkStore(em);
	}
	
	@Test
	public void benchmarkTDBStore() throws Exception {
		EntityDatabase em = new TDBEntityDatabase(id, new VersionedDirectoryProvider(dbDirectory));
		benchmarkStore(em);
	}
	
	@Test
	public void benchmarkBDBStore() throws Exception {
		Serializer serializer = new GzipSerializer( new NTriplesSerializer() );
		BDBEntityDatabase em = new BDBEntityDatabase(dbDirectory.getAbsolutePath(), serializer);
		benchmarkStore(em);
		em.close();
	}
	
	private void checkClearingDatabase(EntityDatabase em) throws Exception{
		int graphs = 10;
		int stmtsPerGraph = 10; 
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			Collection<Quad> cbd = getQuads(graph, subject, stmtsPerGraph);
			quads.addAll(cbd);
			em.put(subject, graph, cbd);
		}
		em.commit();
		
		Collection<Quad> other = em.get(subject);
		assertEquals(quads.size(), other.size());
		
		em.clear();
		assertTrue(em.get(subject).isEmpty());
	}
	
	private void roundTripStatements(EntityDatabase em) throws IOException{
		em.put(subject, graph, quads);
		em.commit();
		Collection<Quad> other = em.get(subject);
		assertEquals(quads.size(), other.size());
		assertTrue(other.containsAll(quads));
	}
	
	private void combineMultipleDescriptions(EntityDatabase em) throws IOException{
		int graphs = 10;
		int stmtsPerGraph = 10; 
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			Collection<Quad> cbd = getQuads(graph, subject, stmtsPerGraph);
			quads.addAll(cbd);
			em.put(subject, graph, cbd);
		}
		em.commit();
		System.out.println(String.format("Stored %s quads across %s graphs", quads.size(), graphs));
		
		Collection<Quad> other = em.get(subject);
		assertEquals(quads.size(), other.size());
		assertTrue(other.containsAll(quads));
		System.out.println(String.format("Fetched %s quads from store", other.size()));
	}
	
	private void benchmarkStore(EntityDatabase em) throws IOException{
		int iter = 5000;
		long start = System.currentTimeMillis();
		for (int i=0; i<iter; i++){
			em.put(subject, graph, quads);
			em.get(subject);	
		}
		em.commit();
		long end = System.currentTimeMillis();
		long duration = end - start;
		System.out.println(String.format("Iterations: %s, Total: %s, PerOp: %s",iter, duration, (double)((double)duration/(double)iter)));
	}
	
	private void initDBDirectory() throws IOException{
		FileUtils.forceMkdir(dbDirectory);
		FileUtils.cleanDirectory(dbDirectory);
	}
	
	private Collection<Quad> getQuads(Node graph, Node subject, int num){
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<num; i++){
			quads.add(
					new Quad(	graph,
								subject,
								Node.createURI("http://example.com/p"),
								Node.createURI("http://example.com/o" + i)));
		}
		return quads;
	}
	
}
