package com.talis.entity.db.ram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;

public class RamEntityDatabase implements EntityDatabase {

	private final Map<String, Collection<Quad>> store = new TreeMap<String, Collection<Quad>>();
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads){
		store.put(getKey(subject, graph), quads);
	}

	@Override
	public void delete(Node subject, Node graph) {
		store.remove(getKey(subject, graph));
	}

	@Override
	public void deleteGraph(Node graph) {
		String targetGraph = graph.toString();
		ArrayList<String> forDeletion = new ArrayList<String>();
		for (String key : store.keySet()){
			String[] parts = key.split("\t");
			String thisGraph = parts[1];
			if (thisGraph.equals(targetGraph)){
				forDeletion.add(key);
			}
		}
		for (String keyToDelete : forDeletion) {
			store.remove(keyToDelete);
		}
	}

	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		String targetSubject = subject.toString();
		Collection<Quad> allQuads = new HashSet<Quad>();
		for (String key : store.keySet()){
			String[] parts = key.split("\t");
			String thisSubject = parts[0];
			if (thisSubject.equals(targetSubject)){
				allQuads.addAll(store.get(key));
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
		// noop
	}
}
