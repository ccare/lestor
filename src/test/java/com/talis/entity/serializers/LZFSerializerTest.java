package com.talis.entity.serializers;

public class LZFSerializerTest extends SerializerTestBase{

	@Override
	Serializer getSerializer() {
		return new LZFSerializer(new POSerializer());
	}

}
