package com.talis.entity.serializers;

public class GzipSerializerTest extends SerializerTestBase{

	@Override
	Serializer getSerializer() {
		return new GzipSerializer(new POSerializer());
	}

}
