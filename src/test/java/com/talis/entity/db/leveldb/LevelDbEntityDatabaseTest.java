package com.talis.entity.db.leveldb;

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

public class LevelDbEntityDatabaseTest extends EntityDatabaseTestBase{

	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();

	@Before
	public void setup() throws Exception{
		System.setProperty(LevelDbEntityDatabase.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		super.setup();
	}
	
	@After
	public void tearDown(){
		System.clearProperty(LevelDbEntityDatabase.DB_LOCATION_PROPERTY);
	}
	
	@Override
	public EntityDatabase getDatabase() {
		return new LevelDbEntityDatabase(new POSerializer(), id);
	}
	
	@Test
	public void getGraph() throws Exception{
		LevelDbEntityDatabase db = new LevelDbEntityDatabase(new POSerializer(), id);
		db.put(subject, graph, quads);
		Node otherGraph = Node.createURI("http://other/graph");
		Collection<Quad> otherQuads = getQuads(otherGraph, subject, 5);
		db.put(subject, otherGraph, otherQuads);
		
		Collection<Quad> allQuads = new ArrayList<Quad>(quads);
		allQuads.addAll(otherQuads);
		
		assertQuadCollectionsEqual(allQuads, db.get(subject));
		assertQuadCollectionsEqual(quads, db.getGraph(graph));
		assertQuadCollectionsEqual(otherQuads, db.getGraph(otherGraph));
	}

	@Test
	public void clearGraph() throws Exception{
		LevelDbEntityDatabase db = new LevelDbEntityDatabase(new POSerializer(), id);
		db.put(subject, graph, quads);
		Node otherGraph = Node.createURI("http://other/graph");
		Collection<Quad> otherQuads = getQuads(otherGraph, subject, 5);
		db.put(subject, otherGraph, otherQuads);

		Collection<Quad> allQuads = db.get(subject);
		assertTrue(allQuads.containsAll(quads));
		assertTrue(allQuads.containsAll(otherQuads));
		
		db.clearGraph(otherGraph);
		
		allQuads = db.get(subject);
		assertTrue(allQuads.containsAll(quads));
		assertFalse(allQuads.containsAll(otherQuads));
	}

}
