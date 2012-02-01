package com.talis.entity.db;

import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.db.ram.RamEntityDatabase;
import com.talis.entity.db.tdb.DatasetProvider;
import com.talis.entity.db.tdb.LocationProvider;
import com.talis.entity.db.tdb.TdbEntityDatabase;
import com.talis.platform.joon.VersionedDirectoryProvider;

public class BenchmarkDatabases {
	
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
	protected Node subject;
	protected Node graph;
	protected Collection<Quad> quads;
	protected String id;
	protected EntityDatabase db;

	@Before
	public void setup(){
		subject = Node.createURI("http://example.com/s");
		graph = Node.createURI("http://example.com/g");
		quads = EntityDatabaseTestBase.getQuads(graph, subject, 30);
		id = "test-id";
	}
	
	@Test
	public void benchmarkRAMStore() throws Exception {
		EntityDatabase db = new RamEntityDatabase();
		benchmarkStore(db);
	}
	
	@Test
	public void benchmarkTDBStore() throws Exception {
		EntityDatabase db = new TdbEntityDatabase(id, new DatasetProvider( new LocationProvider ( new VersionedDirectoryProvider(tmpDir.getRoot()))));
		benchmarkStore(db);
	}
	
	private void benchmarkStore(EntityDatabase db) throws Exception{
		int iter = 100;
//		db.begin();
		long start = System.currentTimeMillis();
		for (int i=0; i<iter; i++){
			db.put(subject, graph, quads);
			db.get(subject);	
		}
//		db.commit();
		long end = System.currentTimeMillis();
		long duration = end - start;
		System.out.println(String.format("Iterations: %s, Total: %s, PerOp: %s",iter, duration, (double)((double)duration/(double)iter)));
	}

}
