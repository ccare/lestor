package com.talis.entity.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.openjena.riot.out.OutputLangUtils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;

public abstract class EntityDatabaseTestBase {
	
	protected Node subject;
	protected Node graph;
	protected Collection<Quad> quads;
	protected String id;
	protected EntityDatabase db;
	
	@Before
	public void setup() throws Exception {
		subject = Node.createURI("http://example.com/s");
		graph = Node.createURI("http://example.com/g");
		quads = getQuads(graph, subject, 30);
		id = "test-id";
		db = getDatabase();
	}
	
	public abstract EntityDatabase getDatabase();
	
	@Test
	public void roundTripStatements() throws Exception{
		long start = System.currentTimeMillis();
		db.put(subject, graph, quads);
		Collection<Quad> other = db.get(subject);
		assertEquals(quads.size(), other.size());
		assertTrue(other.containsAll(quads));
	}
	
	@Test
	public void combineMultipleDescriptionsFromDatabase() throws Exception{
		int graphs = 10;
		int stmtsPerGraph = 10; 
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			Collection<Quad> cbd = getQuads(graph, subject, stmtsPerGraph);
			quads.addAll(cbd);
			db.put(subject, graph, cbd);
		}
		Node excludeMe = Node.createURI("http://example.com/excludeme");
		Collection<Quad> toBeExcluded = getQuads(graph, excludeMe, stmtsPerGraph);
		db.put(excludeMe, graph, toBeExcluded);
		
		Collection<Quad> aggregate = db.get(subject);
		assertEquals(quads.size(), aggregate.size());
		assertTrue(aggregate.containsAll(quads));
		assertFalse(aggregate.containsAll(toBeExcluded));
	}

	@Test
	public void deleteStatementsForSubjectFromSingleGraph() throws Exception {
		Collection<Quad> q1 = new ArrayList<Quad>();
		q1.add(new Quad(graph, 
						subject, 
						Node.createURI("http://example.com/first"), 
						Node.createLiteral("first")));
		
		Node otherGraph = Node.createURI("http://example.com/graphs/other"); 
		Collection<Quad> q2 = new ArrayList<Quad>();
		q2.add(new Quad(otherGraph, 
						subject, 
						Node.createURI("http://example.com/second"), 
						Node.createLiteral("second")));
		db.put(subject, graph, q1);
		db.put(subject, otherGraph, q2);
		
		assertEquals(2, db.get(subject).size());
		db.delete(subject, graph);
		assertEquals(1, db.get(subject).size());
		assertTrue(db.get(subject).containsAll(q2));
	}
		
	@Test
	public void returnEmptyQuadCollectionIfEntityNotStored() throws Exception {
		db.clear();
		assertTrue(db.get(subject).isEmpty());
	}
	
	@Test
	public void clearDatabase() throws Exception {
		int graphs = 10;
		int stmtsPerGraph = 10; 
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			Collection<Quad> cbd = getQuads(graph, subject, stmtsPerGraph);
			quads.addAll(cbd);
			db.put(subject, graph, cbd);
		}
		
		Collection<Quad> other = db.get(subject);
		assertEquals(quads.size(), other.size());
		
		db.clear();
		assertTrue(db.get(subject).isEmpty());	
	}
	
	@Test
	public void deleteGraphSingleGraph() throws Exception {
		int graphs = 1;
		int stmtsPerGraph = 1; 
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			subject = Node.createURI("http://example.com/s" + i);
			Collection<Quad> cbd = getQuads(graph, subject, stmtsPerGraph);
			quads.addAll(cbd);
			db.put(subject, graph, cbd);
		}
		
		subject = Node.createURI("http://example.com/s0");
		graph = Node.createURI("http://example.com/g0");
		
		assertFalse(db.get(subject).isEmpty());
		db.deleteGraph(graph);
		assertTrue(db.get(subject).isEmpty());	
	}
	
	@Test
	public void deleteGraphDoesNotRemoveSubjectsFromOtherGraphs() throws Exception {
		int graphs = 2;
		int stmtsPerGraph = 1; 
		Node subjectInEveryGraph = Node.createURI("http://example.com/s");
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			Collection<Quad> cbd = getQuads(graph, subjectInEveryGraph, stmtsPerGraph);
			quads.addAll(cbd);
			db.put(subjectInEveryGraph, graph, cbd);
		}
		
		graph = Node.createURI("http://example.com/g0");
		
		assertEquals(2, db.get(subject).size());
		db.deleteGraph(graph);
		assertEquals(1, db.get(subject).size());
		assertEquals("http://example.com/g1", ((Quad)db.get(subject).toArray()[0]).getGraph().getURI());
	}
	
	@Test
	public void checkIfSubjectExists() throws Exception {

		assertFalse(db.exists(subject));
		
		int graphs = 2;
		int stmtsPerGraph = 1; 
		Collection<Quad> quads = new ArrayList<Quad>();
		for (int i=0; i<graphs; i++){
			Node graph = Node.createURI("http://example.com/g" + i);
			Collection<Quad> cbd = getQuads(graph, subject, stmtsPerGraph);
			quads.addAll(cbd);
			db.put(subject, graph, cbd);
		}
		
		for (int i=0; i<graphs; i++){
			assertTrue(db.exists(subject));
			Node graph = Node.createURI("http://example.com/g" + i);
			db.deleteGraph(graph);
		}
		assertFalse(db.exists(subject));
	}
	
	public static Collection<Quad> getQuads(Node graph, Node subject, int num){
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
	
	public static void printQuads(Collection<Quad> quads){
		PrintWriter out = new PrintWriter(System.out);
		for (Quad quad : quads){
			OutputLangUtils.output(out, quad, null, null);
		}
		out.flush();
	}
	
	public static void assertQuadCollectionsEqual(Collection<Quad> first, Collection<Quad> second){
		assertEquals(first.size(), second.size());
		assertTrue(first.containsAll(second));
	}
}
