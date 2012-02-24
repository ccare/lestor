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

package com.talis.entity.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCodec implements Codec {

	@Override
	public byte[] encode(byte[] bytes) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		GZIPOutputStream gz = new GZIPOutputStream(b);
		gz.write(bytes);
		gz.flush();
		gz.close();
		return b.toByteArray();
	}

	@Override
	public byte[] decode(byte[] bytes) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
        GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(bytes));
        byte[] buf = new byte[1024];
        int len;
        while ((len = gz.read(buf)) > 0){
        	  b.write(buf, 0, len);
        }
        gz.close();
        b.flush();
        return b.toByteArray();
	}

}