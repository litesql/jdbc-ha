package com.github.litesql.jdbc.ha.client;

import java.time.Instant;
import java.util.Date;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
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
		case "type.googleapis.com/google.protobuf.DoubleValue":
			return com.google.protobuf.DoubleValue.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.FloatValue":
			return com.google.protobuf.FloatValue.parseFrom(x.getValue()).getValue();	
		case "type.googleapis.com/google.protobuf.Int64Value":
			return com.google.protobuf.Int64Value.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.Int32Value":
			return com.google.protobuf.Int32Value.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.UInt64Value":
			return com.google.protobuf.UInt64Value.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.UInt32Value":
			return com.google.protobuf.UInt32Value.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.BoolValue":
			return com.google.protobuf.BoolValue.parseFrom(x.getValue()).getValue();
		case "type.googleapis.com/google.protobuf.Timestamp":
			com.google.protobuf.Timestamp timestamp = com.google.protobuf.Timestamp.parseFrom(x.getValue());
			return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
		case "type.googleapis.com/google.protobuf.BytesValue":
			return com.google.protobuf.BytesValue.parseFrom(x.getValue()).getValue().toByteArray();
		}

		throw new InvalidProtocolBufferException("unsupported type:" + x.toString());
	}

	protected static Any toAny(Object x) {
		if (x == null) {
			return Any.pack(com.google.protobuf.Empty.getDefaultInstance());
		}
		
		if (x instanceof String) {
			String v = (String) x;
			return Any.pack(com.google.protobuf.StringValue.of(v));
		}

		if (x instanceof Double) {
			Double v = (Double) x;
			return Any.pack(com.google.protobuf.DoubleValue.of(v));
		}

		if (x instanceof Float) {
			Float v = (Float) x;
			return Any.pack(com.google.protobuf.FloatValue.of(v));
		}

		if (x instanceof Long) {
			Long v = (Long) x;
			return Any.pack(com.google.protobuf.Int64Value.of(v));
		}

		if (x instanceof Integer) {
			Integer v = (Integer) x;
			return Any.pack(com.google.protobuf.Int32Value.of(v));
		}

		if (x instanceof Boolean) {
			Boolean v = (Boolean) x;
			return Any.pack(com.google.protobuf.BoolValue.of(v));
		}
		
		if (x instanceof Instant) {
			Instant v = (Instant) x;
			return Any.pack(com.google.protobuf.Timestamp.newBuilder().setSeconds(v.getEpochSecond())
					.setNanos(v.getNano()).build());
		}
		
		if (x instanceof Date) {
			Date v = (Date) x;
			return Any.pack(com.google.protobuf.util.Timestamps.fromDate(v));
		}

		if (x instanceof byte[]) {
			byte[] v = (byte[]) x;
			return Any.pack(com.google.protobuf.BytesValue.of(ByteString.copyFrom(v)));
		}

		if (x instanceof ByteString) {
			ByteString v = (ByteString) x;
			return Any.pack(com.google.protobuf.BytesValue.of(v));
		}

		throw new RuntimeException("unsupported type:" + x.getClass().toString());
	}
}
