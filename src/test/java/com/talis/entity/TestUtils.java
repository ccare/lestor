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

package com.talis.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

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
	
	public static void printQuads(Iterable<Quad> quads){
		PrintWriter out = new PrintWriter(System.out);
		for (Quad quad : quads){
			OutputLangUtils.output(out, quad, null, null);
		}
		out.flush();
	}
	
	public static void assertQuadIterablesEqual(Iterable<Quad> first, Iterable<Quad> second){
		Collection<Quad> a = new HashSet<Quad>();
		for (Quad q : first){
			a.add(q);
		}
		Collection<Quad> b = new HashSet<Quad>();
		for (Quad q : second){
			b.add(q);
		}
		assertQuadCollectionsEqual(a, b);
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
