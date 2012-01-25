package com.talis.entity.db.bdb;

import java.util.ArrayList;
import java.util.Collection;

import org.openjena.atlas.lib.Sink;

import com.hp.hpl.jena.sparql.core.Quad;

public class SinkQuadCollector implements Sink<Quad>{

	private final Collection<Quad> quads = new ArrayList<Quad>();

	@Override
	public void send(Quad item) {
		quads.add(item);
	}

	@Override
	public void flush() {/*Do nothing*/}
	@Override
	public void close() {/*Do nothing*/}
	
	public Collection<Quad> getQuads(){
		return quads;
	}
	
}
