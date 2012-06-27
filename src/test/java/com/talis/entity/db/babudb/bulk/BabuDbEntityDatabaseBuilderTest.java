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

package com.talis.entity.db.babudb.bulk;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;

import com.google.common.collect.Iterables;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.compress.SnappyCodec;
import com.talis.entity.db.EntityLoaderSink;
import com.talis.entity.db.babudb.BabuDBFactoryWrapper;
import com.talis.entity.db.babudb.BabuDbEntityDatabase;
import com.talis.entity.db.babudb.DatabaseManager;
import com.talis.entity.marshal.Marshaller;

public class BabuDbEntityDatabaseBuilderTest {

	@Rule
	public TemporaryFolder dbDir0 = new TemporaryFolder();
	
	@Rule
	public TemporaryFolder dbDir1 = new TemporaryFolder();
	
	@Test
	public void verifyBulkLoadedDatabase() throws Exception{
		DatabaseManager dbm0 = new DatabaseManager( dbDir0.getRoot(), new BabuDBFactoryWrapper());
		BabuDbEntityDatabase incremental = new  BabuDbEntityDatabase(new Marshaller(new SnappyCodec()), "first", dbm0);
		EntityLoaderSink sink = new EntityLoaderSink(incremental);
		Tokenizer tokens = TokenizerFactory.makeTokenizerASCII(getQuadStream());
		LangNQuads parser = new LangNQuads(tokens, RiotLib.profile(Lang.NQUADS, null), sink);
		
		long start = System.currentTimeMillis();
		parser.parse();
		sink.flush();
		System.out.println(String.format("Incremental db loaded in %s ms", (System.currentTimeMillis() - start) ) );
		
		File workDir = dbDir1.newFolder("work");
		File outputDir = dbDir1.newFolder("db");
		BabuDbEntityDatabaseBuilder builder = new BabuDbEntityDatabaseBuilder();
		start = System.currentTimeMillis();
		builder.build(getQuadStream(), workDir, outputDir, "second");
		System.out.println(String.format("Bulk load complete in %s ms", (System.currentTimeMillis() - start) ) );
		
		DatabaseManager dbm1 = new DatabaseManager( outputDir, new BabuDBFactoryWrapper());
		BabuDbEntityDatabase bulkloaded = new  BabuDbEntityDatabase(new Marshaller(new SnappyCodec()), "second", dbm1);
		
		Iterable<Entry<Node, Iterable<Quad>>> first = incremental.all();
		Iterator<Entry<Node, Iterable<Quad>>> second = bulkloaded.all().iterator();
		
		System.out.println("Checking database contents"); 
		start = System.currentTimeMillis();
		for (Entry<Node, Iterable<Quad>> fromIncremental : first){
			Entry<Node,Iterable<Quad>> fromBulkLoad = second.next();
			assertEquals(fromIncremental.getKey(), fromBulkLoad.getKey());
			Iterables.elementsEqual(fromIncremental.getValue(), fromBulkLoad.getValue());
		}
		System.out.println(String.format("Databases are equal - check took %s ms", (System.currentTimeMillis() - start)));
	}
	
	private InputStream getQuadStream() throws IOException{
		String resource = "/1-million-quads-srt.nq.gz";
		return new GZIPInputStream(
					new BufferedInputStream(
							this.getClass().getResourceAsStream(resource)));
	}

}
