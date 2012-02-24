package com.talis.entity.compress;

import com.talis.entity.marshal.MarshallerTestBase;

public class NoopCodecTest extends MarshallerTestBase{

	@Override
	protected Codec getCodec(){
		return new NoopCodec();
	}
}
