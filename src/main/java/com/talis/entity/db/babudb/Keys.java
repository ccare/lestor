package com.talis.entity.db.babudb;

import java.io.UnsupportedEncodingException;

import com.hp.hpl.jena.graph.Node;

public class Keys {

	public static byte[] getStorageKey(Node subject, Node graph){
		return getKeyString(subject, graph).getBytes();
	}
	
	public static byte[] getInverseKey(Node subject, Node graph){
		return getKeyString(graph, subject).getBytes();
	}
	
	public static String getKeyString(Node first, Node second){
		return first.getURI().concat("\t").concat(second.getURI());
	}
	
	public static byte[] getKeyPrefix(Node node){
		return node.getURI().concat("\t").getBytes();
	}
	
	public static String asString(byte value[]) {
		if( value == null) {
			return null;
		}
		try {
			return new String(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
