package com.talis.entity.db.ram;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;

public class RamEntityDatabase implements EntityDatabase {

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
		Collection<Quad> allQuads = new HashSet<Quad>();
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
}
