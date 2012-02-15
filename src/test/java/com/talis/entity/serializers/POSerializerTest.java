package com.talis.entity.serializers;

public class POSerializerTest extends SerializerTestBase{

	@Override
	Serializer getSerializer() {
		return new POSerializer();
	}

}
