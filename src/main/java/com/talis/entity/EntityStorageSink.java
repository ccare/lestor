package com.talis.entity;

import java.util.ArrayList;
import java.util.List;

import org.openjena.atlas.lib.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class EntityStorageSink implements Sink<Quad> {

	private static final Logger LOG = LoggerFactory.getLogger(EntityStorageSink.class);
	
	private final EntityDatabase entityManager;

	private final List<Quad> quadBuffer;
	private Node currentSubject;
	private Node currentGraph;
	
	private long quadCount;
	private long flushCount;
	
	public EntityStorageSink(EntityDatabase entityManager){
		this.entityManager = entityManager;
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
				entityManager.put(currentSubject, currentGraph, quadBuffer);
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
