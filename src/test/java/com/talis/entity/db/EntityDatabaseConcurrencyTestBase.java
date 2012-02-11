package com.talis.entity.db;

import static com.talis.entity.db.TestUtils.showMemory;
import static com.talis.entity.db.TestUtils.tryForceGC;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.RiotReader;

import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;

public abstract class EntityDatabaseConcurrencyTestBase {
	
	protected String id;
	protected EntityDatabase[] dbs;
	protected Runtime runtime;
	
	@Before
	public void setup() throws Exception{
		tryForceGC();
		id = "test-id";
		dbs = new EntityDatabase[5];
		dbs[0] = getDatabase();
		dbs[1] = getDatabase();
		dbs[2] = getDatabase();
		dbs[3] = getDatabase();
		dbs[4] = getDatabase();

		System.out.println();
		System.out.println(dbs[0].getClass().getName());
		showMemory();
	}
	
	@After
	public void tearDown() throws Exception{
		tryForceGC();
		showMemory();
		
		for(EntityDatabase db : dbs){
			db.close();
		}
		tryForceGC();
		showMemory();
		System.out.println("=====================================================================================");
	}
	
	public abstract EntityDatabase getDatabase() throws Exception;
	
	@Test
	public void benchmarkLoadingDataset() throws Exception{
		String resource = "/1-million-quads.nq.gz";
//		String resource = "/test-1.nq.gz";
		
		System.out.println("Loading dataset");
		CountDownLatch startGate = new CountDownLatch(1);
		CountDownLatch endGate = new CountDownLatch(dbs.length);
		for (int i=0;i<dbs.length;i++){
			InputStream in = new GZIPInputStream(
								new BufferedInputStream(
									this.getClass().getResourceAsStream(resource)));
			new Thread(new LoadWorker(i, dbs[i], in, startGate, endGate)).start();
		}
		long start = System.currentTimeMillis();
		startGate.countDown();
		endGate.await();
		System.out.println(String.format("Finished %s concurrent loads in %s ms", 
				dbs.length, (System.currentTimeMillis() - start)));
	}
	
	class LoadWorker implements Runnable{
		int id;
		EntityDatabase db;
		InputStream in;
		Sink<Quad> sink;
		CountDownLatch startGate;
		CountDownLatch endGate;
		
		LoadWorker(int id, EntityDatabase db, InputStream in, CountDownLatch startGate, CountDownLatch endGate){
			this.id = id;
			this.db = db;
			this.in = in;
			this.startGate = startGate;
			this.endGate = endGate;
			sink = new EntityLoaderSink(db);
		}
		
		@Override
		public void run() {
//			System.out.println(String.format("Load worker %s ready, waiting to start", id));
			try {
				startGate.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
//			System.out.println("Starting load " + id);
			long start = System.currentTimeMillis();
			RiotReader.createParserNQuads(in, sink).parse();
			sink.flush();
			sink.close();
			System.out.println(String.format("Finished load %s in %s ms", id, (System.currentTimeMillis() - start)));
			endGate.countDown();
		}
	}
	

}
