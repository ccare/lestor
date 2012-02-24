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

package com.talis.entity.db;

import static com.talis.entity.TestUtils.assertQuadIterablesEqual;
import static com.talis.entity.TestUtils.getQuads;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Iterables;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;

public abstract class EntityDatabaseTestBase {
	
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
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
		db.put(subject, graph, quads);
		Iterable<Quad> other = db.get(subject);
		assertQuadIterablesEqual(quads, other);
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
		
		Iterable<Quad> aggregate = db.get(subject);
		assertQuadIterablesEqual(quads, aggregate);
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
		
		assertEquals(2, Iterables.size(db.get(subject)));
		db.delete(subject, graph);
		assertQuadIterablesEqual(q2, db.get(subject));
	}
		
	@Test
	public void returnEmptyQuadCollectionIfEntityNotStored() throws Exception {
		db.clear();
		assertTrue(Iterables.isEmpty(db.get(subject)));
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
		
		Iterable<Quad> other = db.get(subject);
		assertEquals(quads.size(), Iterables.size(other));
		
		db.clear();
		assertTrue(Iterables.isEmpty(db.get(subject)));	
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
		
		assertFalse(Iterables.isEmpty(db.get(subject)));
		db.deleteGraph(graph);
		assertTrue(Iterables.isEmpty(db.get(subject)));	
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
		
		assertEquals(2, Iterables.size(db.get(subject)));
		db.deleteGraph(graph);
		assertEquals(1, Iterables.size(db.get(subject)));
		assertEquals("http://example.com/g1", ((Quad)db.get(subject).iterator().next()).getGraph().getURI());
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
	
	@Test
	public void getGraph() throws Exception{
		db.put(subject, graph, quads);
		Node otherGraph = Node.createURI("http://other/graph");
		Collection<Quad> otherQuads = getQuads(otherGraph, subject, 5);
		db.put(subject, otherGraph, otherQuads);
		
		Collection<Quad> allQuads = new ArrayList<Quad>(quads);
		allQuads.addAll(otherQuads);
		
		assertQuadIterablesEqual(allQuads, db.get(subject));
		assertQuadIterablesEqual(quads, db.getGraph(graph));
		assertQuadIterablesEqual(otherQuads, db.getGraph(otherGraph));
	}
	
	@Test
	public void iterateAllEntities() throws Exception{
		Node secondGraph = Node.createURI(graph.getURI() + "1");
		Node secondSubject = Node.createURI(subject.getURI() + "1");
		Collection<Quad> secondQuads = getQuads(graph, secondSubject, 5);
		Node thirdSubject = Node.createURI(subject.getURI() + "2");
		Collection<Quad> thirdQuads_A = getQuads(graph, thirdSubject, 10);
		Collection<Quad> thirdQuads_B = getQuads(secondGraph, thirdSubject, 10);
		Node fourthSubject = Node.createURI(subject.getURI() + "3");
		Collection<Quad> fourthQuads = getQuads(graph, fourthSubject, 4);
		
		db.put(subject, graph, quads);
		db.put(secondSubject, graph, secondQuads);
		db.put(thirdSubject, graph, thirdQuads_A);
		db.put(thirdSubject, secondGraph, thirdQuads_B);
		db.put(fourthSubject, graph, fourthQuads);
		
		ArrayList<Entry<Node, Iterable<Quad>>> results = new ArrayList<Entry<Node,Iterable<Quad>>>();
 		for (Entry<Node, Iterable<Quad>> entry : db.all()){
			results.add(entry);
		}
 		assertEquals(4, results.size());
 		assertEquals(results.get(0).getKey(), subject);
 		assertQuadIterablesEqual(quads, results.get(0).getValue());
 		assertEquals(results.get(1).getKey(), secondSubject);
 		assertQuadIterablesEqual(secondQuads, results.get(1).getValue());
 		assertEquals(results.get(2).getKey(), thirdSubject);
 		assertQuadIterablesEqual(Iterables.concat(thirdQuads_A, thirdQuads_B), results.get(2).getValue());
 		assertEquals(results.get(3).getKey(), fourthSubject);
 		assertQuadIterablesEqual(fourthQuads, results.get(3).getValue());
	}
	
}
