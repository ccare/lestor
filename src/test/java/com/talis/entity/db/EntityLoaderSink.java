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

package com.talis.entity.db;

import java.util.ArrayList;
import java.util.List;

import org.openjena.atlas.lib.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDatabase;

public class EntityLoaderSink implements Sink<Quad> {

	private static final Logger LOG = LoggerFactory.getLogger(EntityLoaderSink.class);
	
	private final EntityDatabase db;

	private final List<Quad> quadBuffer;
	private Node currentSubject;
	private Node currentGraph;
	
	private long quadCount;
	private long flushCount;
	
	public EntityLoaderSink(EntityDatabase entityManager){
		this.db = entityManager;
		this.quadBuffer = new ArrayList<Quad>();
		currentSubject = Node.NULL;
		currentGraph = Node.NULL;
		quadCount = 0;
		flushCount = 0;
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
		if (++quadCount % 10000 == 0){
			LOG.info("Quads processed : {}", quadCount);
		}
	}
	
	private void flushBuffer(){
		flushCount++;
		if (currentSubject.isBlank()){
			return;
		}
		if (! quadBuffer.isEmpty()){
			try{
				db.put(currentSubject, currentGraph, quadBuffer);
				for (Quad q : db.get(currentSubject)){
					// do nothing
				}
			}catch(Exception e){
				LOG.error("Error storing entity for {} {}", currentSubject + " " + currentGraph, e);
			}
			quadBuffer.clear();
		}
	}

	@Override
	public void flush() {
		flushBuffer();
		LOG.info("Flushing Sink, buffer has been flushed to storage {} times", flushCount);
	}

	@Override
	public void close() {
		flushBuffer();
		LOG.info("Closing Sink, buffer has been flushed to storage {} times", flushCount);
	}
	
	public long getQuadCount(){
		return quadCount;
	}
}