package com.talis.entity;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.out.SinkQuadOutput;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.db.bdb.BDBEntityDatabase;
import com.talis.entity.db.bdb.GzipSerializer;
import com.talis.entity.db.bdb.Serializer;

@SuppressWarnings("PMD")
public class QuadLoader {

	private static final Logger LOG = LoggerFactory.getLogger(QuadLoader.class);
	
	public static void main(String[] args) throws Exception{
		
		String bdbDir = args[0];
		String serialization = args[1];

		String input = "stdin";
		InputStream in = System.in;
		if (args.length == 3){
			input = args[2];
			in = new FileInputStream(new File(input));
		}	
		
		LOG.info("Starting load of quads from {} into BDB store at {} with {}", 
				new Object[]{ input, bdbDir, serialization });
		
		serialization = "com.talis.entity.serialize." + serialization;
		Serializer serializer = (Serializer) Class.forName(serialization).newInstance();
		
		File bdbDirectory = new File(bdbDir);
		initBDBDirectory(bdbDirectory);
		
		
		EntityDatabase db = new BDBEntityDatabase(bdbDirectory.getAbsolutePath(), new GzipSerializer(serializer));
		EntityStorageSink sink = new EntityStorageSink(db);
		Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(in);
		LangNQuads parser = new LangNQuads(tokenizer, RiotLib.profile(Lang.NQUADS, null), sink);
		
		LOG.info("Store initialised, starting load");
		long startTime = System.currentTimeMillis();
        parser.parse();
        long endTime = System.currentTimeMillis();
		LOG.info("Load completed, analysing database");
		
		double loadTime = (double)(((double)endTime - (double)startTime) / (double)1000);
		double loadRate = (double) sink.getQuadCount() / loadTime;
		long bytesOnDisk = FileUtils.sizeOfDirectory(bdbDirectory);
		String dbSize = FileUtils.byteCountToDisplaySize(bytesOnDisk);
		
		LOG.info("Loaded {} quads in {} seconds. {} TPS, {} ({}) on disk", new Object[] { sink.getQuadCount(), loadTime, loadRate, bytesOnDisk, dbSize } );
	}
	
	private static void initBDBDirectory(File bdbDirectory) throws IOException{
		FileUtils.forceMkdir(bdbDirectory);
		FileUtils.cleanDirectory(bdbDirectory);
	}
	
	public static void dumpCBD(EntityDatabase em, String uri) throws Exception{
		SinkQuadOutput out = new SinkQuadOutput(System.out);
		for (Quad q : em.get(Node.createURI(uri))){
			out.send(q);
		}
		out.flush();
		out.close();
	}
	
}
