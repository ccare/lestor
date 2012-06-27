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
