package com.talis.entity.compress;

import java.io.IOException;

public interface Codec {

	public byte[] encode(byte[] bytes) throws IOException ;
	public byte[] decode(byte[] bytes) throws IOException ;
	
}
