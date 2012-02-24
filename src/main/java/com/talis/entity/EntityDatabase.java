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

import java.util.Collection;
import java.util.Map.Entry;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public interface EntityDatabase {
	
	public void put(Node subject, Node graph, Collection<Quad> quads) throws EntityDatabaseException;
	
	public void delete(Node subject, Node graph) throws EntityDatabaseException;
	public void deleteGraph(Node graph) throws EntityDatabaseException;
	
	public boolean exists(Node subject) throws EntityDatabaseException;
	public Iterable<Quad> get(Node subject) throws EntityDatabaseException;
	public Iterable<Quad> getGraph(Node graph) throws EntityDatabaseException;
	
	public Iterable<Entry<Node, Iterable<Quad>>> all() throws EntityDatabaseException;
	
	public void clear() throws EntityDatabaseException;
	public void close() throws EntityDatabaseException;
	
	public void begin() throws EntityDatabaseException;
	public void commit() throws EntityDatabaseException;
	public void abort() throws EntityDatabaseException;
	
}
