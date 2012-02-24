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

import static com.talis.entity.TestUtils.showMemory;
import static com.talis.entity.TestUtils.tryForceGC;

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
	public static final int NUM_DBS = 5;
	
	@Before
	public void setup() throws Exception{
		tryForceGC();
		id = "test-id";
		dbs = new EntityDatabase[NUM_DBS];
		for(int i=0;i<NUM_DBS;i++){
			dbs[i] = getDatabase();
		}
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
		for (int i=0;i<NUM_DBS;i++){
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
			try {
				startGate.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long start = System.currentTimeMillis();
			RiotReader.createParserNQuads(in, sink).parse();
			sink.flush();
			sink.close();
			System.out.println(String.format("Finished load %s in %s ms", id, (System.currentTimeMillis() - start)));
			endGate.countDown();
		}
	}
	

}
