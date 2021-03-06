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

package com.talis.entity.db.babudb.bulk;

import java.util.ArrayList;
import java.util.List;

import org.openjena.atlas.lib.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDesc;
import com.talis.entity.marshal.Marshaller;

public class EntitySink implements Sink<Quad>{

	private static final Logger LOG = LoggerFactory.getLogger(EntitySink.class);
	
	private final List<Quad> quadBuffer;
	private final Marshaller marshaller;
	
	private Node currentSubject;
	private Node currentGraph;
	private EntityDesc entity;
	
	private long quadCount;
	private long entityCount;
	
	public EntitySink (Marshaller marshaller){
		LOG.debug("Initialising sink");
		this.quadBuffer = new ArrayList<Quad>();
		this.marshaller = marshaller;
	}
	
	@Override
	public void send(Quad quad) {
	if ( ! quad.getSubject().equals(currentSubject) ||
			  ! quad.getGraph().equals(currentGraph ) )
		{
		    flushBuffer();		
		    currentSubject = quad.getSubject();
		    currentGraph = quad.getGraph();
		}
		quadBuffer.add(quad);
	}

	@Override
	public void flush() {
		flushBuffer();
		LOG.debug("Flushing Sink, buffer has been flushed to storage {} times", entityCount);
	}

	@Override
	public void close() {
		flush();
		LOG.debug("Closing sink (saw {} entities from {} quads", entityCount, quadCount);
	}
	
	public void reset(){
		entity = null;
	}
	
	public boolean hasEntity(){
		return entity != null;
	}
	
	public EntityDesc getEntity(){
		if (null == entity){
			throw new IllegalStateException("No Entity to return");
		}
		return entity;
	}
		
	private void flushBuffer(){
		if (null == currentSubject || currentSubject.isBlank()){
			return;
		}
		if (! quadBuffer.isEmpty()){
			try{
				entity = marshaller.toEntityDesc(currentSubject, currentGraph, quadBuffer);
			}catch(Exception e){
				LOG.error("Error storing entity for {} {}", currentSubject + " " + currentGraph, e);
			}
			quadBuffer.clear();
		}
		entityCount++;
	}
	
	public long getQuadCount(){
		return quadCount;
	}

	
}
