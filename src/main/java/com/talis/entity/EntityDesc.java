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

package com.talis.entity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	public String toString(){
		return (subject == null ? "null" : subject.getURI()) 
					+ ("->") 
						+ (graph == null ? "null" : graph.getURI());
	}
	
	static class Serializer implements com.talis.sort.Serializer<EntityDesc>{
        private static final Logger LOG = LoggerFactory.getLogger(EntityDesc.class);
		@Override
		
		public byte[] toBytes(EntityDesc entity) {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			try{
				writeNodeOrNull(entity.subject, out);
				writeNodeOrNull(entity.graph, out);
				writeByteArrayOrNull(entity.bytes, out);
				return bytes.toByteArray();
			}catch(IOException e){
				LOG.error("IO Error serializing entity {}", entity);
				throw new RuntimeException("Error serializing entity", e);
			}
		}
		
		private void writeNodeOrNull(Node node, DataOutputStream out) throws IOException{
			if (null == node){
				out.write(0);
			}else{
				writeByteArrayOrNull(node.getURI().getBytes(), out);
			}
		}
		
		private void writeByteArrayOrNull(byte[] bytes, DataOutputStream out) throws IOException{
			if (null == bytes){
				out.writeInt(0);
			}else{
				out.writeInt(bytes.length);
				out.write(bytes);
			}
		}
		
		private byte[] readByteArrayOrNull(DataInputStream in) throws IOException{
			int length = in.readInt();
			byte[] bytes = new byte[length];
			in.readFully(bytes);
			return bytes;
		}

		@Override
		public EntityDesc toObject(byte[] bytes) {
			EntityDesc entity = new EntityDesc();
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
			try{
				byte[] subject = readByteArrayOrNull(in);
				if (subject.length > 0){
					entity.subject = Node.createURI(new String(subject));
				}
				byte[] graph = readByteArrayOrNull(in);
				if (graph.length > 0){
					entity.graph = Node.createURI(new String(graph));
				}
				byte[] body = readByteArrayOrNull(in);
				if (body.length > 0){
					entity.bytes = body;
				}
				return entity;
			}catch(IOException e){
				LOG.error("IO Error deserializing entity {}", entity);
				throw new RuntimeException("Error deserializing entity", e);
			}
		}
	}
}
