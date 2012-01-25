package com.talis.entity.db.bdb;

import com.hp.hpl.jena.graph.Node;

public class EntityDesc {
	public Node subject;
	public Node graph;
	public byte[] bytes;
	
	public EntityDesc(){
		subject = null;
		graph = null;
		bytes = null;
	}
	
	public EntityDesc(Node subject, Node graph, byte[] bytes){
		this.subject = subject;
		this.graph = graph;
		this.bytes = bytes;
	}
}
