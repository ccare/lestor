package com.talis.entity;

import java.util.Collection;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public interface EntityDatabase {
	
	public void put(Node subject, Node graph, Collection<Quad> quads) throws EntityDatabaseException;
	public void delete(Node subject, Node graph) throws EntityDatabaseException;
	public Collection<Quad> get(Node subject) throws EntityDatabaseException;
	public void clear() throws EntityDatabaseException;

	public void begin() throws EntityDatabaseException;
	public void commit() throws EntityDatabaseException;
	public void abort() throws EntityDatabaseException;
	
	public void close() throws EntityDatabaseException;
}
