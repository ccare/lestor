package com.talis.entity.db.bdb;

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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TwoTuple other = (TwoTuple) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

}
