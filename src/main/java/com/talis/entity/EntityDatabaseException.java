package com.talis.entity;

public class EntityDatabaseException extends Exception {

	public EntityDatabaseException(String message, Throwable cause) {
		super(message, cause);
	}

	public EntityDatabaseException(String message) {
		super(message);
	}

}
