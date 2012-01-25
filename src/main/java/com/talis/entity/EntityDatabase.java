package com.talis.entity;

import java.io.IOException;
import java.util.Collection;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public interface EntityDatabase {
	
	public void put(Node subject, Node graph, Collection<Quad> quads) throws IOException;
	public void delete(Node subject, Node graph) throws IOException;
	public Collection<Quad> get(Node subject) throws IOException;
	public void clear() throws IOException;
	public void commit();
	
}
