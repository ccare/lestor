package com.talis.entity.db.ram;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;

public class RAMEntityDatabase implements EntityDatabase {

	private final Map<String, Collection<Quad>> store = new TreeMap<String, Collection<Quad>>();
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		store.put(getKey(subject, graph), quads);
	}

	@Override
	public void delete(Node subject, Node graph) {
		store.remove(getKey(subject, graph));
	}

	@Override
	public Collection<Quad> get(Node subject) throws IOException {
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
	
	private String getKey(Node subject, Node graph){
		return subject.getURI().concat("\t").concat(graph.getURI());
	}

	@Override
	public void clear() throws IOException {
		store.clear();
	}

	@Override
	public void commit() {
		// do nothing
	}

}
