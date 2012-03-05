package com.talis.entity.db.babudb;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

import org.openjena.riot.out.EscapeStr;

import com.hp.hpl.jena.graph.Node;

public class Keys {

	private static EscapeStr escapeProc = new EscapeStr(true);

	public static byte[] getStorageKey(Node subject, Node graph){
		return getKeyString(subject, graph).getBytes();
	}
	
	public static byte[] getInverseKey(Node subject, Node graph){
		return getKeyString(graph, subject).getBytes();
	}
	
	public static String getKeyString(Node first, Node second){
		StringWriter w1 = new StringWriter();
		StringWriter w2 = new StringWriter();
		escapeProc.writeStr(w1, first.getURI());
		escapeProc.writeStr(w2, second.getURI());
		return w1.toString().concat("\t").concat(w2.toString());
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
