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

import build.buf.gen.sql.v1.TypedValue;
import build.buf.gen.sql.v1.ValueType;

import com.github.litesql.jdbc.ha.HAException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class Converter {

	public static Object fromAny(TypedValue x) throws InvalidProtocolBufferException {
		if (x == null || x.getType() == ValueType.VALUE_TYPE_NULL
				|| x.getType() == ValueType.VALUE_TYPE_UNSPECIFIED) {
			return null;
		}
		switch (x.getType()) {
		case VALUE_TYPE_STRING:
			return x.getStringVal();
		case VALUE_TYPE_DOUBLE:
			return x.getDoubleVal();
		case VALUE_TYPE_INT64:
			return x.getInt64Val();
		case VALUE_TYPE_UINT64:
			return BigInteger.valueOf(x.getUint64Val()).and(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE));
		case VALUE_TYPE_BOOL:
			return x.getBoolVal();
		case VALUE_TYPE_TIMESTAMP:
			long nanos = x.getInt64Val();
			if (nanos == 0L && !x.getStringVal().isEmpty()) {
				return x.getStringVal();
			}
			return Instant.ofEpochSecond(Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
		case VALUE_TYPE_BYTES:
			return x.getBytesVal().toByteArray();
		default:
			throw new InvalidProtocolBufferException("Unsupported TypedValue type: " + x.getType());
		}
	}

	public static TypedValue toAny(Object x) throws SQLException {
		if (x == null) {
			return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_NULL).build();
		}

		try {
			if (x instanceof String) {
				return stringValue((String) x);
			}

			if (x instanceof HAReaderInput) {
				String s = ((HAReaderInput) x).readString();
				return s == null ? nullValue() : stringValue(s);
			}

			if (x instanceof HAStreamInput) {
				HAStreamInput streamInput = (HAStreamInput) x;
				if (streamInput.isText()) {
					String s = streamInput.readString();
					return s == null ? nullValue() : stringValue(s);
				}
				byte[] bytes = streamInput.readBytes();
				return bytes == null ? nullValue() : bytesValue(bytes);
			}

			if (x instanceof Reader) {
				String s = new HAReaderInput((Reader) x, -1).readString();
				return s == null ? nullValue() : stringValue(s);
			}

			if (x instanceof InputStream) {
				byte[] bytes = new HAStreamInput((InputStream) x, -1).readBytes();
				return bytes == null ? nullValue() : bytesValue(bytes);
			}

			if (x instanceof Clob) {
				Clob clob = (Clob) x;
				long len = clob.length();
				String s = len == 0 ? "" : new HAReaderInput(clob.getCharacterStream(), len).readString();
				return s == null ? nullValue() : stringValue(s);
			}

			if (x instanceof Blob) {
				Blob blob = (Blob) x;
				long len = blob.length();
				byte[] bytes = len == 0 ? new byte[0] : new HAStreamInput(blob.getBinaryStream(), len).readBytes();
				return bytesValue(bytes);
			}

			if (x instanceof SQLXML) {
				return stringValue(((SQLXML) x).getString());
			}

			if (x instanceof RowId) {
				return bytesValue(((RowId) x).getBytes());
			}

			if (x instanceof URL || x instanceof UUID || x instanceof LocalDate || x instanceof LocalTime
					|| x instanceof LocalDateTime || x instanceof java.sql.Date || x instanceof java.sql.Time) {
				return stringValue(x.toString());
			}

			if (x instanceof Double) {
				return doubleValue((Double) x);
			}

			if (x instanceof Float) {
				return doubleValue(((Float) x).doubleValue());
			}

			if (x instanceof BigDecimal) {
				return doubleValue(((BigDecimal) x).doubleValue());
			}

			if (x instanceof Long) {
				return int64Value((Long) x);
			}

			if (x instanceof AtomicLong) {
				return int64Value(((AtomicLong) x).get());
			}

			if (x instanceof BigInteger) {
				BigInteger bi = (BigInteger) x;
				if (bi.bitLength() <= 63) {
					return int64Value(bi.longValue());
				}
				return stringValue(bi.toString());
			}

			if (x instanceof Integer) {
				return int64Value(((Integer) x).longValue());
			}
			if (x instanceof Short) {
				return int64Value(((Short) x).longValue());
			}
			if (x instanceof Byte) {
				return int64Value(((Byte) x).longValue());
			}
			if (x instanceof AtomicInteger) {
				return int64Value(((AtomicInteger) x).longValue());
			}

			if (x instanceof Boolean) {
				return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_BOOL).setBoolVal((Boolean) x).build();
			}

			if (x instanceof Timestamp) {
				return timestampValue(((Timestamp) x).toInstant());
			}

			if (x instanceof Instant) {
				return timestampValue((Instant) x);
			}

			if (x instanceof OffsetDateTime) {
				return timestampValue(((OffsetDateTime) x).toInstant());
			}

			if (x instanceof ZonedDateTime) {
				return timestampValue(((ZonedDateTime) x).toInstant());
			}

			if (x instanceof Date) {
				return timestampValue(Instant.ofEpochMilli(((Date) x).getTime()));
			}

			if (x instanceof byte[]) {
				return bytesValue((byte[]) x);
			}

			if (x instanceof ByteString) {
				return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_BYTES).setBytesVal((ByteString) x).build();
			}
		} catch (IOException e) {
			throw new HAException("Failed to read stream parameter", e);
		}

		throw new HAException("Unsupported parameter type: " + x.getClass().getName());
	}

	private static TypedValue nullValue() {
		return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_NULL).build();
	}

	private static TypedValue stringValue(String value) {
		return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_STRING).setStringVal(value).build();
	}

	private static TypedValue bytesValue(byte[] value) {
		return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_BYTES)
				.setBytesVal(ByteString.copyFrom(value)).build();
	}

	private static TypedValue doubleValue(double value) {
		return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_DOUBLE).setDoubleVal(value).build();
	}

	private static TypedValue int64Value(long value) {
		return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_INT64).setInt64Val(value).build();
	}

	private static TypedValue timestampValue(Instant instant) {
		try {
			long nanos = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L),
					instant.getNano());
			return TypedValue.newBuilder().setType(ValueType.VALUE_TYPE_TIMESTAMP).setInt64Val(nanos).build();
		} catch (ArithmeticException e) {
			return stringValue(instant.toString());
		}
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
					|| value instanceof java.sql.Date || value instanceof java.sql.Time || value instanceof UUID
					|| value instanceof URL) {
				return value.toString();
			}
			return value;
		} catch (IOException e) {
			throw new HAException("Failed to read stream parameter", e);
		}
	}
}
