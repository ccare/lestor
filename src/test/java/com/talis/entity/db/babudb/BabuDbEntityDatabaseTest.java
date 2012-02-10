package com.talis.entity.db.babudb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.db.EntityDatabaseTestBase;
import com.talis.entity.serializers.POSerializer;
import com.talis.entity.serializers.SnappySerializer;

public class BabuDbEntityDatabaseTest extends EntityDatabaseTestBase {
	
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
	@Before
	public void setup() throws Exception{
		System.setProperty(DatabaseManager.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		super.setup();
	}
	
	@After
	public void tearDown(){
		System.clearProperty(DatabaseManager.DB_LOCATION_PROPERTY);
	}
	
	@Override
	public EntityDatabase getDatabase() {
		DatabaseManager dbManager = new DatabaseManager();
		return new BabuDbEntityDatabase(new SnappySerializer(new POSerializer()), id, dbManager);
	}
	
	@Test
	public void getGraph() throws Exception{
		BabuDbEntityDatabase bbDb = (BabuDbEntityDatabase)db;
		bbDb.put(subject, graph, quads);
		Node otherGraph = Node.createURI("http://other/graph");
		Collection<Quad> otherQuads = getQuads(otherGraph, subject, 5);
		bbDb.put(subject, otherGraph, otherQuads);
		
		Collection<Quad> allQuads = new ArrayList<Quad>(quads);
		allQuads.addAll(otherQuads);
		
		assertQuadCollectionsEqual(allQuads, bbDb.get(subject));
		assertQuadCollectionsEqual(quads, bbDb.getGraph(graph));
		assertQuadCollectionsEqual(otherQuads, bbDb.getGraph(otherGraph));
	}

	@Test
	public void clearGraph() throws Exception{
		BabuDbEntityDatabase bbDb = (BabuDbEntityDatabase)db;
		bbDb.put(subject, graph, quads);
		Node otherGraph = Node.createURI("http://other/graph");
		Collection<Quad> otherQuads = getQuads(otherGraph, subject, 5);
		bbDb.put(subject, otherGraph, otherQuads);

		Collection<Quad> allQuads = bbDb.get(subject);
		assertTrue(allQuads.containsAll(quads));
		assertTrue(allQuads.containsAll(otherQuads));
		
		bbDb.clearGraph(otherGraph);
		
		allQuads = bbDb.get(subject);
		assertTrue(allQuads.containsAll(quads));
		assertFalse(allQuads.containsAll(otherQuads));
	}

}
