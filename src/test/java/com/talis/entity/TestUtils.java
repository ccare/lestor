package com.talis.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.openjena.riot.out.OutputLangUtils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class TestUtils {

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
	
	public static void tryForceGC(){
		Runtime r = Runtime.getRuntime();
		for (int i=0;i<10;i++){
			r.gc();
		}
	}
	
	public static void showMemory(){
		Runtime r = Runtime.getRuntime();
		long bytes = r.totalMemory() - r.freeMemory();
		System.out.println(String.format("Memory usage : %s (%s)", FileUtils.byteCountToDisplaySize(bytes), bytes));
	}
}
