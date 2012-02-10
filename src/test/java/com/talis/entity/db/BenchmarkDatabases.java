package com.talis.entity.db;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.db.babudb.BabuDbEntityDatabase;
import com.talis.entity.db.krati.KratiEntityDatabase;
import com.talis.entity.db.leveldb.LevelDbEntityDatabase;
import com.talis.entity.db.leveldb.LevelDbEntityDatabase2;
import com.talis.entity.db.ram.RamEntityDatabase;
import com.talis.entity.db.tdb.DatasetProvider;
import com.talis.entity.db.tdb.LocationProvider;
import com.talis.entity.db.tdb.TdbEntityDatabase;
import com.talis.entity.serializers.GzipSerializer;
import com.talis.entity.serializers.LZFSerializer;
import com.talis.entity.serializers.POSerializer;
import com.talis.entity.serializers.Serializer;
import com.talis.entity.serializers.SnappySerializer;
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
	
	@After
	public void tearDown(){
		System.out.println(tmpDir.getRoot());
		long size = FileUtils.sizeOfDirectory(tmpDir.getRoot());
		System.out.println(String.format("Size on disk : %s (%s)", FileUtils.byteCountToDisplaySize(size), size));
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
	
	@Test
	public void benchmarkLevelDbStore() throws Exception {
		System.setProperty(LevelDbEntityDatabase.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		EntityDatabase db = new LevelDbEntityDatabase(new POSerializer(),id);
		benchmarkStore(db);
	}
	
	@Test
	public void benchmarkLevelDbStore2() throws Exception {
		System.setProperty(LevelDbEntityDatabase2.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		EntityDatabase db = new LevelDbEntityDatabase2(new POSerializer(),id);
		benchmarkStore(db);
	}
	
	@Test
	public void benchmarkKratiStore() throws Exception {
		System.setProperty(KratiEntityDatabase.DB_LOCATION_PROPERTY, tmpDir.getRoot().getAbsolutePath());
		EntityDatabase db = new KratiEntityDatabase(new POSerializer(),id);
		benchmarkStore(db);
	}
	
	@Test
	public void benchmarkBabuDbStore() throws Exception {
		Serializer s = new SnappySerializer(new POSerializer());
//		Serializer s = new POSerializer();
//		Serializer s = new LZFSerializer(new POSerializer());
//		Serializer s = new GzipSerializer(new POSerializer());
		
		EntityDatabase db = new BabuDbEntityDatabase(s, id, new VersionedDirectoryProvider(tmpDir.getRoot()));
		benchmarkStore(db);
		
		
		
		File f = new File(System.getProperty("java.io.tmpdir"), "keep");
		FileUtils.forceMkdir(f);
		FileUtils.cleanDirectory(f);
		FileUtils.copyDirectory(tmpDir.getRoot(), f);
		EntityDatabase db2 = new BabuDbEntityDatabase(s, id, new VersionedDirectoryProvider(tmpDir.getRoot()));
		EntityDatabaseTestBase.printQuads(db2.get(subject));
	}
	
	private void benchmarkStore(EntityDatabase db) throws Exception{
		System.out.println("======================================");
		System.out.println(db.getClass().getName());
		System.out.println(tmpDir.getRoot());
		int iter = 100000;
		db.begin();
		long start = System.currentTimeMillis();
		long last = start;
		for (int i=0; i<iter; i++){
			if (i%1000 == 0){
				long now = System.currentTimeMillis();
				System.out.println("Run " + i + " iterations in " + (now - last) + " ms");
				last = now;
			}
//			Node s = Node.createURI(subject + "_ " + i%2);
//			Node g = Node.createURI(graph + "_" + i%10);
//			db.put( s, g, quads);
//			db.get( s );	
			db.put( subject, graph, quads);
			db.get( subject );	
		}
		db.commit();
		long end = System.currentTimeMillis();
		long duration = end - start;

		System.out.println(String.format("Iterations: %s, Total: %s, PerOp: %s",iter, duration, (double)((double)duration/(double)iter)));
	}

}
