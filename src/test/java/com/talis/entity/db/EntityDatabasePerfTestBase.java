package com.talis.entity.db;

import static com.talis.entity.db.TestUtils.getQuads;
import static com.talis.entity.db.TestUtils.showMemory;
import static com.talis.entity.db.TestUtils.tryForceGC;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;

public abstract class EntityDatabasePerfTestBase {
	
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
	protected Node subject;
	protected Node graph;
	protected Collection<Quad> quads;
	protected String id;
	protected EntityDatabase db;
	protected Runtime runtime;
	
	@Before
	public void setup() throws Exception{
		tryForceGC();
		subject = Node.createURI("http://example.com/s");
		graph = Node.createURI("http://example.com/g");
		quads = getQuads(graph, subject, 30);
		id = "test-id";
		db = getDatabase();
		System.out.println();
		System.out.println(db.getClass().getName());
		showMemory();
	}
	
	@After
	public void tearDown() throws Exception{
		tryForceGC();
		showMemory();
		
		db.close();
		tryForceGC();
		showMemory();
		System.out.println("=====================================================================================");
	}
	
	public abstract EntityDatabase getDatabase();
	
	@Test
	public void benchmarkAccessSingleGraph() throws EntityDatabaseException{
		System.out.println("Accessing Single Graph");
		int graphs = 100000;
		int stmtsPerGraph = 20;
		long start = System.currentTimeMillis();
		db.begin();
		for (int i=0;i<graphs; i++){
			Node thisGraph = Node.createURI(graph.getURI() + "_" + i);
			db.put( subject, thisGraph, getQuads(thisGraph, subject, stmtsPerGraph));
		}
		db.commit();
		System.out.println(String.format("Populated %s graphs (%s total statements) in %s ms",
								graphs, graphs * stmtsPerGraph, (System.currentTimeMillis() - start)));
		
		int iter = Math.round(graphs / 4);
		Random r = new Random();
		start = System.currentTimeMillis();
		for (int i=0; i<iter; i++){
			Node thisGraph = Node.createURI(graph.getURI() + "_" + r.nextInt(graphs));
			db.getGraph( thisGraph );	
		}
		
		long end = System.currentTimeMillis();
		long duration = end - start;
		System.out.println(String.format("Iterations: %s, Total: %s, PerOp: %s",iter, duration, (double)((double)duration/(double)iter)));
		long size = FileUtils.sizeOfDirectory(tmpDir.getRoot());
		System.out.println(String.format("Size on disk : %s (%s)", FileUtils.byteCountToDisplaySize(size), size));
	}
	
	@Test
	public void clearingSingleSmallGraphFromMany() throws EntityDatabaseException{
		System.out.println("Clearing Single Small Graph In Many");
		testClearingSingleGraph(100000, 20);
	}
	
	@Test
	public void clearingSingleLargeGraphFromFew() throws EntityDatabaseException{
		System.out.println("Clearing Single Large Graph In Few");
		testClearingSingleGraph(20, 100000);
	}
		
	private void testClearingSingleGraph(int graphs, int stmtsPerGraph) throws EntityDatabaseException{
		
		int stmtsPerSubject = 20;
		int subjectCount = Math.round(stmtsPerGraph / stmtsPerSubject);
		long start = System.currentTimeMillis();
		db.begin();
		
		int quadCount = 0;
		
		for (int i=0;i<graphs; i++){
			Node thisGraph = Node.createURI(graph.getURI() + "/" + i);
			for (int j=0;j<subjectCount;j++){
				Node thisSubject = Node.createURI(subject.getURI() + "/" + i + "/" + j);
				db.put( thisSubject, thisGraph, getQuads(thisGraph, thisSubject, stmtsPerSubject));
				quadCount += stmtsPerSubject;
			}
		}
		db.commit();
		System.out.println(String.format("Populated %s graphs (%s total statements) in %s ms",
								graphs, quadCount, (System.currentTimeMillis() - start)));
				
		int iter = Math.round(graphs / 4);
		Set<Node> deleted = new HashSet<Node>();
		Random r = new Random();
		start = System.currentTimeMillis();
		while(deleted.size() < iter){
			Node thisGraph = Node.createURI(graph.getURI() + "/" + r.nextInt(graphs));
			if (!deleted.contains(thisGraph)){
				db.deleteGraph( thisGraph );
				deleted.add(thisGraph);
			}
		}
		
		long end = System.currentTimeMillis();
		long duration = end - start;
		System.out.println(String.format("Iterations: %s, Total: %s, PerOp: %s",iter, duration, (double)((double)duration/(double)iter)));
		long size = FileUtils.sizeOfDirectory(tmpDir.getRoot());
		System.out.println(String.format("Size on disk : %s (%s)", FileUtils.byteCountToDisplaySize(size), size));
		
	}
	
	@Test
	public void benchmarkRoundTripping() throws Exception{
		System.out.println("Round trip quads");
		int iter = 10000;
		db.begin();
		long start = System.currentTimeMillis();
		for (int i=0; i<iter; i++){
			db.put( subject, graph, quads);
			db.get( subject );	
		}
		db.commit();
		long end = System.currentTimeMillis();
		long duration = end - start;

		System.out.println(String.format("Iterations: %s, Total: %s, PerOp: %s",iter, duration, (double)((double)duration/(double)iter)));
		long size = FileUtils.sizeOfDirectory(tmpDir.getRoot());
		System.out.println(String.format("Size on disk : %s (%s)", FileUtils.byteCountToDisplaySize(size), size));
		
	}

}
