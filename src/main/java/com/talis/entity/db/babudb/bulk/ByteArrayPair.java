package com.talis.entity.db.babudb.bulk;

import java.util.Map.Entry;

public class ByteArrayPair implements Entry<Object, Object> {

	private final byte[] key;
    private final byte[] value;
    
    public ByteArrayPair(byte[] key, byte[] value){
        this.key = key;
        this.value = value;
    }
    
    @Override
    public byte[] getKey() {
        return key;
    }
    
    @Override
    public byte[] getValue() {
        return value;
    }
    
    @Override
    public byte[] setValue(Object value) {
        throw new UnsupportedOperationException();
    }
	
}
