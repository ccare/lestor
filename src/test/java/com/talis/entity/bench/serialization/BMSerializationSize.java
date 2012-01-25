package com.talis.entity.bench.serialization;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openjena.riot.out.OutputLangUtils;
import org.openjena.riot.out.SinkQuadOutput;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.db.bdb.EntityDesc;
import com.talis.entity.db.bdb.GzipSerializer;
import com.talis.entity.db.bdb.LZFSerializer;
import com.talis.entity.db.bdb.NQuadsSerializer;
import com.talis.entity.db.bdb.NTriplesSerializer;
import com.talis.entity.db.bdb.POSerializer;
import com.talis.entity.db.bdb.Serializer;
import com.talis.entity.db.bdb.SnappySerializer;

public class BMSerializationSize {
	
	Node graph;
	Node subject;
	
	@Before
	public void setup(){
		graph = Node.createURI(uriWithFixedPrefix("http://example.com/graphs").toString());
		subject = Node.createURI(uriWithFixedPrefix("http://example.com/resources").toString());
	}
	
	private String testSerializer(Serializer ser, Collection<Quad> q) throws IOException{
		EntityDesc serialized = ser.serialize( subject, graph, q);
		long sizeInBytes = serialized.bytes.length;
		int runs = 1000;
		double avgSerTime = getAverageSerializationTime(ser, subject, graph, q, runs);
		double avgDesTime = getAverageDeserializationTime(ser, serialized, runs);
		return String.format("%s\t%s\t%s", sizeInBytes, avgSerTime, avgDesTime);
	}
	
	public static String padRight(String s, int n) {
	     return String.format("%1$-" + n + "s", s);  
	}
	
	@Test
	public void bar() throws IOException{
		Collection<Quad> q = makeQuads(100, 10, 50);
		FileOutputStream fos = new FileOutputStream(new File("test.nq"));
		SinkQuadOutput sink = new SinkQuadOutput(fos);
		for (Quad quad : q){
			sink.send(quad);
		}
		sink.flush();
		sink.close();
		fos.flush();
		fos.close();
	}
	
	@Test
	public void compareSerializers() throws IOException{
		Collection<Quad> q = makeQuads(100, 10, 50);
		
		Serializer ser = new NQuadsSerializer();
		System.out.println(padRight("NQuadsSerializer: ", 25) + testSerializer(ser, q));
		
		ser = new NTriplesSerializer();
		System.out.println(padRight("NTriplesSerializer: ", 25) + testSerializer(ser, q));
		
		ser = new POSerializer();
		System.out.println(padRight("POSerializer: ", 25) + testSerializer(ser, q));
			
		ser = new GzipSerializer( new NQuadsSerializer() );
		System.out.println(padRight("GZipped NQuads: ", 25) + testSerializer(ser, q));
		
		ser = new GzipSerializer( new NTriplesSerializer() );
		System.out.println(padRight("GZipped NTriples: ", 25) + testSerializer(ser, q));
		
		ser = new GzipSerializer( new POSerializer() );
		System.out.println(padRight("GZipped PO: ", 25) + testSerializer(ser, q));
		
		ser = new LZFSerializer( new NQuadsSerializer() );
		System.out.println(padRight("LZFed NQuads: ", 25) + testSerializer(ser, q));
		
		ser = new LZFSerializer( new NTriplesSerializer() );
		System.out.println(padRight("LZFed NTriples: ", 25) + testSerializer(ser, q));
		
		ser = new LZFSerializer( new POSerializer() );
		System.out.println(padRight("LZFed PO: ", 25) + testSerializer(ser, q));
		
		ser = new SnappySerializer( new NQuadsSerializer() );
		System.out.println(padRight("Snappy NQuads: ", 25) + testSerializer(ser, q));
		
		ser = new SnappySerializer( new NTriplesSerializer() );
		System.out.println(padRight("Snappy NTriples: ", 25) + testSerializer(ser, q));
		
		ser = new SnappySerializer( new POSerializer() );
		System.out.println(padRight("Snappy PO: ", 25) + testSerializer(ser, q));		
		
	}
	
//	@Test
//	public void testSizeInBytes() throws IOException{
//		Collection<Quad> q = makeQuads(100, 10, 50);
//		
//		NQuadsSerializer nqs = new NQuadsSerializer();
//		byte[] nqb = nqs.serialize(q);
//		System.out.println("NQuadsSerializer: " + nqb.length);
//		
//		NTriplesSerializer nts = new NTriplesSerializer(graph);
//		byte[] ntb = nts.serialize(q);
//		System.out.println("NTriplesSerializer: " + ntb.length);
//		
//		POSerializer pos = new POSerializer(subject, graph);
//		byte[] pob = pos.serialize(q);
//		System.out.println("POSerializer: " + pob.length);
//			
//		GzipSerializer znqs = new GzipSerializer( new NQuadsSerializer() );
//		byte[] znqb = znqs.serialize(q);
//		System.out.println("GZipped NQuads: " + znqb.length);
//		
//		GzipSerializer znts = new GzipSerializer( new NTriplesSerializer(graph) );
//		byte[] zntb = znts.serialize(q);
//		System.out.println("GZipped NTriples: " + zntb.length);
//		
//		GzipSerializer zpos = new GzipSerializer( new POSerializer(subject, graph) );
//		byte[] zpob = zpos.serialize(q);
//		System.out.println("GZipped PO: " + zpob.length);
//		
//		LZFSerializer lnqs = new LZFSerializer( new NQuadsSerializer() );
//		byte[] lnqb = lnqs.serialize(q);
//		System.out.println("LZFed NQuads: " + lnqb.length);
//		
//		LZFSerializer lnts = new LZFSerializer( new NTriplesSerializer(graph) );
//		byte[] lntb = lnts.serialize(q);
//		System.out.println("LZFed NTriples: " + lntb.length);
//		
//		LZFSerializer lpos = new LZFSerializer( new POSerializer(subject, graph) );
//		byte[] lpob = lpos.serialize(q);
//		System.out.println("LZFed PO: " + lpob.length);
//		
//		SnappySerializer snqs = new SnappySerializer( new NQuadsSerializer() );
//		byte[] snqb = snqs.serialize(q);
//		System.out.println("Snappy NQuads: " + snqb.length);
//		
//		SnappySerializer snts = new SnappySerializer( new NTriplesSerializer(graph) );
//		byte[] sntb = snts.serialize(q);
//		System.out.println("Snappy NTriples: " + sntb.length);
//		
//		SnappySerializer spos = new SnappySerializer( new POSerializer(subject, graph) );
//		byte[] spob = spos.serialize(q);
//		System.out.println("Snappy PO: " + spob.length);
//	}
	
	@Test @Ignore
	public void foo() throws Exception {
		Collection<Quad> q = makeQuads(100, 10, 150);
		Serializer ser = new POSerializer();
		dump(q);
		System.out.println("=================================================");
		dump(ser.deserialize(ser.serialize( subject, graph, q )));
		System.out.println("=================================================");
	}
	
//	@Test
//	public void testSerializationTime() throws IOException{
//		Collection<Quad> q = makeQuads(100, 10, 150);
//		int runs = 1000;
//		
//		Serializer ser = new NQuadsSerializer();
//		System.out.println("NQuadsSerializer: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new NTriplesSerializer(graph);
//		System.out.println("NTriplesSerializer: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new POSerializer(subject, graph);
//		System.out.println("POSerializer: " + getAverageSerializationTime(ser, q, runs));
//			
//		ser = new GzipSerializer( new NQuadsSerializer() );
//		System.out.println("GZipped NQuads: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new GzipSerializer( new NTriplesSerializer(graph) );
//		System.out.println("GZipped NTriples: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new GzipSerializer( new POSerializer(subject, graph) );
//		System.out.println("GZipped PO: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new LZFSerializer( new NQuadsSerializer() );
//		System.out.println("LZFed NQuads: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new LZFSerializer( new NTriplesSerializer(graph) );
//		System.out.println("LZFed NTriples: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new LZFSerializer( new POSerializer(subject, graph) );
//		System.out.println("LZFed PO: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new SnappySerializer( new NQuadsSerializer() );
//		System.out.println("Snappy NQuads: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new SnappySerializer( new NTriplesSerializer(graph) );
//		System.out.println("Snappy NTriples: " + getAverageSerializationTime(ser, q, runs));
//		
//		ser = new SnappySerializer( new POSerializer(subject, graph) );
//		System.out.println("Snappy PO: " + getAverageSerializationTime(ser, q, runs));		
//	}
//	
//	
//	@Test
//	public void testDeserializationTime() throws IOException{
//		Collection<Quad> q = makeQuads(100, 10, 150);
//		int runs = 1000;
//		
//		Serializer ser = new NQuadsSerializer();
//		System.out.println("NQuadsSerializer: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new NTriplesSerializer(graph);
//		System.out.println("NTriplesSerializer: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new POSerializer(subject, graph);
//		System.out.println("POSerializer: " + getAverageDeserializationTime(ser, q, runs));
//			
//		ser = new GzipSerializer( new NQuadsSerializer() );
//		System.out.println("GZipped NQuads: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new GzipSerializer( new NTriplesSerializer(graph) );
//		System.out.println("GZipped NTriples: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new GzipSerializer( new POSerializer(subject, graph) );
//		System.out.println("GZipped PO: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new LZFSerializer( new NQuadsSerializer() );
//		System.out.println("LZFed NQuads: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new LZFSerializer( new NTriplesSerializer(graph) );
//		System.out.println("LZFed NTriples: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new LZFSerializer( new POSerializer(subject, graph) );
//		System.out.println("LZFed PO: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new SnappySerializer( new NQuadsSerializer() );
//		System.out.println("Snappy NQuads: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new SnappySerializer( new NTriplesSerializer(graph) );
//		System.out.println("Snappy NTriples: " + getAverageDeserializationTime(ser, q, runs));
//		
//		ser = new SnappySerializer( new POSerializer(subject, graph) );
//		System.out.println("Snappy PO: " + getAverageDeserializationTime(ser, q, runs));		
//	}

	
	private double getAverageDeserializationTime(Serializer ser, EntityDesc serialized, int runs) throws IOException{
		byte[] serializedBytes = Arrays.copyOf(serialized.bytes, serialized.bytes.length);
		long totalTime = 0;
		for (int i=0;i<runs;i++){
			long start = System.currentTimeMillis();
			serialized.bytes = serializedBytes;
			ser.deserialize(serialized);
			totalTime += System.currentTimeMillis() - start;
		}
		return (double)totalTime /(double)runs;
	}
	
	private double getAverageSerializationTime(Serializer ser, Node subject, Node graph, Collection<Quad> quads, int runs) throws IOException{
		for (int i=0;i<100;i++){
			ser.serialize(subject, graph, quads);
		}
		long totalTime = 0;
		for (int i=0;i<runs;i++){
			long start = System.currentTimeMillis();
			ser.serialize(subject, graph, quads);
			totalTime += System.currentTimeMillis() - start;
		}
		return (double)totalTime /(double)runs;
	}
	
	public void dump(Collection<Quad> quads){
		PrintWriter w = new PrintWriter(System.out);
		for (Quad q : quads){
			OutputLangUtils.output(w, q, null, null);
		}
		w.flush();
	}
	
	public Collection<Quad> makeQuads(int count, int schemaCount, int maxLiteralLen ){
		Random r = new Random();
		Collection<Quad> quads = new ArrayList<Quad>();
		
		String[] schemaPrefixes = new String[schemaCount];
		for (int i=0; i < schemaCount; i++){
			schemaPrefixes[i] = String.format("http://%s.com/schemas/%s/p", randomAlphabetic(10).toLowerCase(), randomAlphabetic(10)); 
		}
		
		for (int i=0;i<count; i++){
			Node p = Node.createURI( uriWithFixedPrefix( schemaPrefixes[ r.nextInt(schemaCount)] ).toString() );
			Node o = null; 
			if (r.nextInt(10) < 4){
				o = makeLiteral( maxLiteralLen ) ;
			}else{
				o = Node.createURI(String.format("http://%s.com/path/%s", randomAlphabetic(10).toLowerCase(), randomAlphabetic(10)));
			}
			
			Quad q = new Quad( graph, subject, p, o );
			quads.add(q);
		}
		return quads;
	}

	public URI uriWithFixedPrefix(String prefix){
		String uri = String.format("%s/%s", prefix, randomAlphabetic(15));
		return URI.create(uri);
	}
	
	public Node makeLiteral(int len){
		return Node.createLiteral(randomAlphabetic(len));
	}
	
	
}
