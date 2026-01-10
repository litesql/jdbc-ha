package com.github.litesql.jdbc.driver.ha.client;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

public class Converter {

	protected static Object fromAny(Any x) throws InvalidProtocolBufferException {
		if (x == null) {
			return null;
		}
		switch (x.getTypeUrl()) {
		case "type.googleapis.com/google.protobuf.Empty":
			return null;
		case "type.googleapis.com/google.protobuf.StringValue":
			return com.google.protobuf.StringValue.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.Int64Value":
			return com.google.protobuf.Int64Value.parseFrom(x.getValue()).getValue();
		}
		
		return null;	
	}
	
	protected static Any toAny(Object x) {
		if (x == null) {
			return null;
		}
		if (x instanceof Integer) {
		    Integer i = (Integer) x;
		    return Any.pack(com.google.protobuf.Int64Value.of(i));
		} else if (x instanceof String) {
		    String s = (String) x;
		    return Any.pack(com.google.protobuf.StringValue.of(s));
		}
		
		return null;
		
	}
}
