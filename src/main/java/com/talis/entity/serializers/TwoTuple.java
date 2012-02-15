package com.talis.entity.serializers;

import com.hp.hpl.jena.graph.Node;

public class TwoTuple {
	
	private final Node first;
	private final Node second;
	
	public TwoTuple(Node first, Node second){
		this.first = first;
		this.second = second;
	}
	
	@Override
	public String toString() {
		return "[" + first + "," + second + "]";
	}
	
	public Node getFirst(){
		return first;
	}
	
	public Node getSecond(){
		return second;
	}
	
}
