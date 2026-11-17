package com.github.litesql.jdbc.ha.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.github.litesql.jdbc.ha.HAException;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class Converter {

	public static Object fromAny(Any x) throws InvalidProtocolBufferException {
		if (x == null || x.getTypeUrl().isEmpty()) {
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
		default:
			throw new InvalidProtocolBufferException("Unsupported protobuf Any type: " + x.getTypeUrl());
		}
	}

	public static Any toAny(Object x) throws SQLException {
		if (x == null) {
			return Any.pack(com.google.protobuf.Empty.getDefaultInstance());
		}

		try {
			if (x instanceof String) {
				return Any.pack(com.google.protobuf.StringValue.of((String) x));
			}

			if (x instanceof HAReaderInput) {
				String s = ((HAReaderInput) x).readString();
				return s == null ? Any.pack(com.google.protobuf.Empty.getDefaultInstance())
						: Any.pack(com.google.protobuf.StringValue.of(s));
			}

			if (x instanceof HAStreamInput) {
				HAStreamInput streamInput = (HAStreamInput) x;
				if (streamInput.isText()) {
					String s = streamInput.readString();
					return s == null ? Any.pack(com.google.protobuf.Empty.getDefaultInstance())
							: Any.pack(com.google.protobuf.StringValue.of(s));
				}
				byte[] bytes = streamInput.readBytes();
				return bytes == null ? Any.pack(com.google.protobuf.Empty.getDefaultInstance())
						: Any.pack(com.google.protobuf.BytesValue.of(ByteString.copyFrom(bytes)));
			}

			if (x instanceof Reader) {
				String s = new HAReaderInput((Reader) x, -1).readString();
				return Any.pack(com.google.protobuf.StringValue.of(s));
			}

			if (x instanceof InputStream) {
				byte[] bytes = new HAStreamInput((InputStream) x, -1).readBytes();
				return Any.pack(com.google.protobuf.BytesValue.of(ByteString.copyFrom(bytes)));
			}

			if (x instanceof Clob) {
				Clob clob = (Clob) x;
				long len = clob.length();
				String s = len == 0 ? "" : new HAReaderInput(clob.getCharacterStream(), len).readString();
				return Any.pack(com.google.protobuf.StringValue.of(s));
			}

			if (x instanceof Blob) {
				Blob blob = (Blob) x;
				long len = blob.length();
				byte[] bytes = len == 0 ? new byte[0] : new HAStreamInput(blob.getBinaryStream(), len).readBytes();
				return Any.pack(com.google.protobuf.BytesValue.of(ByteString.copyFrom(bytes)));
			}

			if (x instanceof SQLXML) {
				return Any.pack(com.google.protobuf.StringValue.of(((SQLXML) x).getString()));
			}

			if (x instanceof RowId) {
				return Any.pack(com.google.protobuf.BytesValue.of(ByteString.copyFrom(((RowId) x).getBytes())));
			}

			if (x instanceof URL || x instanceof UUID || x instanceof LocalDate || x instanceof LocalTime
					|| x instanceof LocalDateTime) {
				return Any.pack(com.google.protobuf.StringValue.of(x.toString()));
			}

			if (x instanceof Double) {
				return Any.pack(com.google.protobuf.DoubleValue.of((Double) x));
			}

			if (x instanceof Float) {
				return Any.pack(com.google.protobuf.FloatValue.of((Float) x));
			}

			if (x instanceof BigDecimal) {
				return Any.pack(com.google.protobuf.DoubleValue.of(((BigDecimal) x).doubleValue()));
			}

			if (x instanceof Long) {
				return Any.pack(com.google.protobuf.Int64Value.of((Long) x));
			}

			if (x instanceof AtomicLong) {
				return Any.pack(com.google.protobuf.Int64Value.of(((AtomicLong) x).get()));
			}

			if (x instanceof BigInteger) {
				BigInteger bi = (BigInteger) x;
				if (bi.bitLength() <= 63) {
					return Any.pack(com.google.protobuf.Int64Value.of(bi.longValue()));
				}
				return Any.pack(com.google.protobuf.StringValue.of(bi.toString()));
			}

			if (x instanceof Integer || x instanceof Short || x instanceof Byte || x instanceof AtomicInteger) {
				return Any.pack(com.google.protobuf.Int32Value.of(((Number) x).intValue()));
			}

			if (x instanceof Boolean) {
				return Any.pack(com.google.protobuf.BoolValue.of((Boolean) x));
			}

			if (x instanceof Timestamp) {
				Timestamp ts = (Timestamp) x;
				return Any.pack(com.google.protobuf.Timestamp.newBuilder()
						.setSeconds(Math.floorDiv(ts.getTime(), 1000L)).setNanos(ts.getNanos()).build());
			}

			if (x instanceof Instant) {
				Instant v = (Instant) x;
				return Any.pack(com.google.protobuf.Timestamp.newBuilder().setSeconds(v.getEpochSecond())
						.setNanos(v.getNano()).build());
			}

			if (x instanceof OffsetDateTime) {
				Instant v = ((OffsetDateTime) x).toInstant();
				return Any.pack(com.google.protobuf.Timestamp.newBuilder().setSeconds(v.getEpochSecond())
						.setNanos(v.getNano()).build());
			}

			if (x instanceof ZonedDateTime) {
				Instant v = ((ZonedDateTime) x).toInstant();
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
		} catch (IOException e) {
			throw new HAException("Failed to read stream parameter", e);
		}

		throw new HAException("Unsupported parameter type: " + x.getClass().getName());
	}

	public static Object unwrapForLocalJdbc(Object value) throws SQLException {
		if (value == null) {
			return null;
		}
		try {
			if (value instanceof HAReaderInput) {
				return ((HAReaderInput) value).readString();
			}
			if (value instanceof HAStreamInput) {
				HAStreamInput si = (HAStreamInput) value;
				return si.isText() ? si.readString() : si.readBytes();
			}
			if (value instanceof Instant || value instanceof OffsetDateTime || value instanceof ZonedDateTime
					|| value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime
					|| value instanceof UUID || value instanceof URL) {
				return value.toString();
			}
			return value;
		} catch (IOException e) {
			throw new HAException("Failed to read stream parameter", e);
		}
	}
}
