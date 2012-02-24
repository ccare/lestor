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

package com.talis.entity.db.ram;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;

public class RamEntityDatabase implements EntityDatabase {

	public static final Comparator<? super Quad> QUAD_COMPARATOR = new Comparator<Quad>(){
		@Override
		public int compare(Quad a, Quad b) {
			int cmp = a.getSubject().toString().compareTo(b.getSubject().toString()); 
			if (cmp != 0){
				return cmp;
			}
			cmp = a.getPredicate().toString().compareTo(b.getPredicate().toString()); 
			if (cmp != 0){
				return cmp;
			}
			cmp = a.getObject().toString().compareTo(b.getObject().toString()); 
			if (cmp != 0){
				return cmp;
			}
			return a.getGraph().toString().compareTo(b.getGraph().toString()); 
		}
	};

	private static final Comparator<? super Node> NODE_COMPARATOR = new Comparator<Node>(){
		@Override
		public int compare(Node o1, Node o2) {
			return (o1.getURI().compareTo(o2.getURI()));
		}
	};
	
	private Map<String, Collection<Quad>> store = new TreeMap<String, Collection<Quad>>();
	private Map<Node, Set<Node>> graphIndex = new HashMap<Node, Set<Node>>();
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads){
		String key = getKey(subject, graph); 
		store.put(key, quads);
		Set<Node> subjects = graphIndex.get(graph);
		if (null == subjects){
			subjects = new HashSet<Node>();
			graphIndex.put(graph, subjects);
		}
		subjects.add(subject);
	}

	@Override
	public void delete(Node subject, Node graph) {
		store.remove(getKey(subject, graph));
		graphIndex.remove(getKey(graph, subject));
	}

	@Override
	public void deleteGraph(Node graph) {
		Set<Node> subjects = graphIndex.get(graph);
		if (null != subjects){
			for (Node subjectToDelete : subjects) {
				store.remove(getKey(subjectToDelete, graph));
			}
		}
		graphIndex.remove(graph);
	}

	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		String targetSubject = subject.toString();
		Collection<Quad> allQuads = new TreeSet<Quad>(QUAD_COMPARATOR);
		for (String key : store.keySet()){
			String[] parts = key.split("\t");
			String thisSubject = parts[0];
			if (thisSubject.equals(targetSubject)){
				allQuads.addAll(store.get(thisSubject + "\t" + parts[1]));
			}
		}
		return allQuads;
	}
	
	@Override
	public Collection<Quad> getGraph(Node graph) throws EntityDatabaseException {
		Collection<Quad> allQuads = new HashSet<Quad>();
		Set<Node> subjects = graphIndex.get(graph);
		if (null != subjects){
			for (Node subject : subjects) {
				allQuads.addAll(store.get(getKey(subject, graph)));
			}
		}
		return allQuads;
	}
	@Override
	public boolean exists(Node subject) throws EntityDatabaseException {
		String targetSubject = subject.toString();
		for (String key : store.keySet()){
			String[] parts = key.split("\t");
			String thisSubject = parts[0];
			if (thisSubject.equals(targetSubject)){
				return true;
			}
		}
		return false;
	}
	
	private String getKey(Node subject, Node graph){
		return subject.getURI().concat("\t").concat(graph.getURI());
	}

	@Override
	public void clear() throws EntityDatabaseException {
		store.clear();
		graphIndex.clear();
	}

	@Override
	public void commit() {
		// noop
	}

	@Override
	public void begin(){
		//noop
	}

	@Override
	public void abort(){
		throw new UnsupportedOperationException("Not supported");
	}

	@Override 
	public void close(){
		store = null;
		graphIndex = null;
	}

	@Override
	public Iterable<Entry<Node, Iterable<Quad>>> all()
			throws EntityDatabaseException {
		String previous = null;
		Map<Node, Iterable<Quad>> everything = new TreeMap<Node, Iterable<Quad>>(NODE_COMPARATOR);
		for (String key : store.keySet()){
			String subject = key.split("\t")[0];
			if (key != previous){
			  Node subjectURI = Node.createURI(subject);
			  everything.put(subjectURI, get(subjectURI));
			}
		}
		return everything.entrySet();
	}
}
