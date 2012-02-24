package com.talis.entity.compress;

import java.io.IOException;

public class NoopCodec implements Codec {

	@Override
	public byte[] encode(byte[] bytes) throws IOException {
		return bytes;
	}

	@Override
	public byte[] decode(byte[] bytes) throws IOException {
		return bytes;
	}

}
