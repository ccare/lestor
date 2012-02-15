package com.talis.entity.serializers;

public class SnappySerializerTest extends SerializerTestBase{

	@Override
	Serializer getSerializer() {
		return new SnappySerializer(new POSerializer());
	}

}
