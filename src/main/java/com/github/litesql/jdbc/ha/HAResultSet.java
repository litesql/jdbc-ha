package com.github.litesql.jdbc.ha;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.utils.CommonUtils;

import com.dbeaver.jdbc.model.AbstractJdbcResultSet;
import com.github.litesql.jdbc.ha.client.HAExecutionResult;

public class HAResultSet extends AbstractJdbcResultSet<HAStatement, HAResultSetMetaData> {

	@Nullable
	private final HAExecutionResult result;
	private transient int cursor = 0;
	private transient boolean closed;
	private transient boolean wasNull;
	private transient Map<String, Integer> nameMap;

	public HAResultSet(@NotNull HAStatement statement, @Nullable HAExecutionResult result) {
		super(statement, null);
		this.result = result;
	}

	@Nullable
	public HAExecutionResult getResult() {
		return result;
	}

	private void checkClosed() throws SQLException {
		if (this.closed) {
			throw new HAException("ResultSet is closed");
		}
	}

	private List<Object[]> rows() {
		return result != null ? result.getRows() : Collections.emptyList();
	}

	private List<String> columns() {
		return result != null ? result.getColumns() : Collections.emptyList();
	}

	private int getColumnIndex(String columnLabel) throws SQLException {
		checkClosed();
		if (columnLabel == null) {
			throw new HAException("Column label must not be null");
		}
		if (nameMap == null) {
			nameMap = new HashMap<>();
			List<String> columns = columns();
			for (int i = 0; i < columns.size(); i++) {
				String col = columns.get(i);
				if (col != null) {
					nameMap.putIfAbsent(col.toUpperCase(Locale.ENGLISH), i + 1);
				}
			}
		}
		Integer index = nameMap.get(columnLabel.toUpperCase(Locale.ENGLISH));
		if (index == null) {
			throw new HAException("Column '" + columnLabel + "' is not present in result set");
		}
		return index;
	}

	private Object[] getCurrentRow() throws SQLException {
		checkClosed();
		List<Object[]> rows = rows();
		if (cursor < 1) {
			throw new HAException("Fetch not started");
		}
		if (cursor > rows.size()) {
			throw new HAException("Fetch ended");
		}
		return rows.get(cursor - 1);
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();
		int size = rows().size();
		if (cursor <= size) {
			cursor++;
		}
		return cursor <= size;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkClosed();
		return !rows().isEmpty() && cursor == 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		checkClosed();
		return !rows().isEmpty() && cursor > rows().size();
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkClosed();
		return !rows().isEmpty() && cursor == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		checkClosed();
		return !rows().isEmpty() && cursor == rows().size();
	}

	@Override
	public int getRow() throws SQLException {
		checkClosed();
		return (cursor >= 1 && cursor <= rows().size()) ? cursor : 0;
	}

	void closeInternal() {
		this.closed = true;
	}

	@Override
	public void close() throws SQLException {
		if (!this.closed) {
			this.closed = true;
			if (this.statement != null) {
				this.statement.onResultSetClosed();
			}
		}
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkClosed();
		return wasNull;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		if (val == null) {
			return null;
		}
		if (val instanceof byte[]) {
			return new String((byte[]) val, StandardCharsets.UTF_8);
		}
		return val.toString();
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		if (val == null) {
			return false;
		}
		if (val instanceof Number) {
			return ((Number) val).intValue() != 0;
		}
		if (val instanceof String) {
			String s = ((String) val).trim();
			if ("1".equals(s)) {
				return true;
			}
			if ("0".equals(s)) {
				return false;
			}
		}
		return CommonUtils.toBoolean(val);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		return val == null ? 0 : (byte) CommonUtils.toInt(val);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		return val == null ? 0 : (short) CommonUtils.toInt(val);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		return val == null ? 0 : CommonUtils.toInt(val);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		return val == null ? 0L : CommonUtils.toLong(val);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		return val == null ? 0.0f : CommonUtils.toFloat(val);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Object val = getObject(columnIndex);
		return val == null ? 0.0d : CommonUtils.toDouble(val);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		Object object = getObject(columnIndex);
		if (object == null) {
			return null;
		}
		if (object instanceof BigDecimal) {
			return (BigDecimal) object;
		}
		if (object instanceof BigInteger) {
			return new BigDecimal((BigInteger) object);
		}
		if (object instanceof Long || object instanceof Integer || object instanceof Short || object instanceof Byte) {
			return BigDecimal.valueOf(((Number) object).longValue());
		}
		String str = object.toString().trim();
		if (str.isEmpty()) {
			return null;
		}
		try {
			return new BigDecimal(str);
		} catch (NumberFormatException e) {
			throw new HAException("Invalid BigDecimal value: " + str, e);
		}
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		BigDecimal bd = getBigDecimal(columnIndex);
		return bd == null ? null : bd.setScale(scale, RoundingMode.HALF_UP);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		Object object = getObject(columnIndex);
		if (object == null) {
			return null;
		}
		if (object instanceof byte[]) {
			return (byte[]) object;
		}
		return object.toString().getBytes(StandardCharsets.UTF_8);
	}

	private static final DateTimeFormatter FLEXIBLE_OFFSET_DATETIME = new DateTimeFormatterBuilder()
			.append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
			.optionalStart().appendLiteral(' ').optionalEnd()
			.optionalStart().appendOffset("+HH:MM:ss", "Z").optionalEnd()
			.optionalStart().appendOffset("+HHMMss", "Z").optionalEnd()
			.optionalStart().appendOffset("+HH", "Z").optionalEnd()
			.toFormatter(Locale.ROOT);

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getDate(columnIndex, null);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		Object object = getRawObject(columnIndex);
		if (object == null) {
			return null;
		}
		ZoneId zone = cal != null ? cal.getTimeZone().toZoneId() : ZoneId.systemDefault();
		if (object instanceof Date) {
			return (Date) object;
		}
		if (object instanceof Timestamp) {
			return toSqlDate(((Timestamp) object).toInstant(), cal);
		}
		if (object instanceof java.util.Date) {
			return toSqlDate(Instant.ofEpochMilli(((java.util.Date) object).getTime()), cal);
		}
		if (object instanceof Instant) {
			return toSqlDate((Instant) object, cal);
		}
		if (object instanceof Number) {
			return toSqlDate(Instant.ofEpochMilli(((Number) object).longValue()), cal);
		}
		String s = object.toString().trim();
		if (s.isEmpty()) {
			return null;
		}
		if (isNumeric(s)) {
			return toSqlDate(Instant.ofEpochMilli(Long.parseLong(s)), cal);
		}
		try {
			if (s.length() == 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
				LocalDate ld = LocalDate.parse(s);
				if (cal == null) {
					return Date.valueOf(ld);
				}
				return new Date(ld.atStartOfDay(zone).toInstant().toEpochMilli());
			}
			Timestamp ts = parseTimestampString(s, zone);
			return toSqlDate(ts.toInstant(), cal);
		} catch (Exception e) {
			throw new HAException("Invalid Date value: " + s, e);
		}
	}

	private static Date toSqlDate(Instant instant, @Nullable Calendar cal) {
		if (cal != null) {
			LocalDate ld = instant.atZone(cal.getTimeZone().toZoneId()).toLocalDate();
			return Date.valueOf(ld);
		}
		// Pure DATE values scanned by Go SQLite drivers arrive as midnight UTC (00:00:00Z).
		// Keep UTC date for exact midnight UTC so negative offsets (e.g. -03:00) don't shift back 1 day.
		ZoneId effectiveZone = (instant.getNano() == 0 && Math.floorMod(instant.getEpochSecond(), 86_400L) == 0L)
				? ZoneOffset.UTC
				: ZoneId.systemDefault();
		return Date.valueOf(instant.atZone(effectiveZone).toLocalDate());
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return getTime(columnIndex, null);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		Object object = getRawObject(columnIndex);
		if (object == null) {
			return null;
		}
		ZoneId zone = cal != null ? cal.getTimeZone().toZoneId() : ZoneId.systemDefault();
		if (object instanceof Time) {
			return (Time) object;
		}
		if (object instanceof Timestamp) {
			LocalTime lt = ((Timestamp) object).toInstant().atZone(zone).toLocalTime();
			return Time.valueOf(lt);
		}
		if (object instanceof java.util.Date) {
			return new Time(((java.util.Date) object).getTime());
		}
		if (object instanceof Instant) {
			LocalTime lt = ((Instant) object).atZone(zone).toLocalTime();
			return Time.valueOf(lt);
		}
		if (object instanceof Number) {
			return new Time(((Number) object).longValue());
		}
		String s = object.toString().trim();
		if (s.isEmpty()) {
			return null;
		}
		if (isNumeric(s)) {
			return new Time(Long.parseLong(s));
		}
		try {
			if (!s.contains("-") && s.contains(":")) {
				return Time.valueOf(LocalTime.parse(s));
			}
			Timestamp ts = parseTimestampString(s, zone);
			return Time.valueOf(ts.toInstant().atZone(zone).toLocalTime());
		} catch (Exception e) {
			throw new HAException("Invalid Time value: " + s, e);
		}
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getTimestamp(columnIndex, null);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		Object object = getRawObject(columnIndex);
		if (object == null) {
			return null;
		}
		ZoneId zone = cal != null ? cal.getTimeZone().toZoneId() : ZoneId.systemDefault();
		if (object instanceof Timestamp) {
			return (Timestamp) object;
		}
		if (object instanceof java.util.Date) {
			return new Timestamp(((java.util.Date) object).getTime());
		}
		if (object instanceof Instant) {
			return Timestamp.from((Instant) object);
		}
		if (object instanceof Number) {
			return new Timestamp(((Number) object).longValue());
		}
		String s = object.toString().trim();
		if (s.isEmpty()) {
			return null;
		}
		if (isNumeric(s)) {
			return new Timestamp(Long.parseLong(s));
		}
		return parseTimestampString(s, zone);
	}

	private static boolean isNumeric(String s) {
		int start = (s.startsWith("-") || s.startsWith("+")) ? 1 : 0;
		if (start >= s.length()) {
			return false;
		}
		for (int i = start; i < s.length(); i++) {
			if (!Character.isDigit(s.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	private static Timestamp parseTimestampString(String raw, ZoneId zone) throws SQLException {
		String s = stripGoMonotonicAndZoneName(raw.trim());
		try {
			return Timestamp.from(Instant.parse(s));
		} catch (DateTimeParseException ignored) {
		}
		try {
			return Timestamp.from(OffsetDateTime.parse(s).toInstant());
		} catch (DateTimeParseException ignored) {
		}
		String normalized = normalizeDateTimeSeparator(s);
		try {
			return Timestamp.from(Instant.parse(normalized));
		} catch (DateTimeParseException ignored) {
		}
		try {
			return Timestamp.from(OffsetDateTime.parse(normalized).toInstant());
		} catch (DateTimeParseException ignored) {
		}
		try {
			return Timestamp.from(OffsetDateTime.parse(normalized, FLEXIBLE_OFFSET_DATETIME).toInstant());
		} catch (DateTimeParseException ignored) {
		}
		try {
			LocalDateTime ldt = LocalDateTime.parse(normalized);
			return Timestamp.from(ldt.atZone(zone).toInstant());
		} catch (DateTimeParseException ignored) {
		}
		try {
			LocalDate ld = LocalDate.parse(s);
			return Timestamp.from(ld.atStartOfDay(zone).toInstant());
		} catch (DateTimeParseException ignored) {
		}
		try {
			return Timestamp.valueOf(s);
		} catch (IllegalArgumentException e) {
			throw new HAException("Invalid Timestamp value: " + raw, e);
		}
	}

	private static String normalizeDateTimeSeparator(String s) {
		if (s.length() > 10 && s.charAt(4) == '-' && s.charAt(7) == '-' && s.charAt(10) == ' ') {
			return s.substring(0, 10) + 'T' + s.substring(11);
		}
		return s;
	}

	private static String stripGoMonotonicAndZoneName(String s) {
		int monoIdx = s.indexOf(" m=");
		if (monoIdx > 0) {
			s = s.substring(0, monoIdx).trim();
		}
		// Handle Go time.Time.String() trailing zone abbreviation, e.g. "2026-11-17 10:15:30.123 -0300 -03" or "... +0000 UTC"
		if (s.length() > 20 && s.charAt(4) == '-' && s.charAt(7) == '-') {
			int lastSpace = s.lastIndexOf(' ');
			if (lastSpace > 18) {
				String beforeLast = s.substring(0, lastSpace).trim();
				int prevSpace = beforeLast.lastIndexOf(' ');
				if (prevSpace >= 18) {
					String offsetCandidate = beforeLast.substring(prevSpace + 1);
					if ((offsetCandidate.startsWith("+") || offsetCandidate.startsWith("-"))
							&& offsetCandidate.length() >= 3) {
						return beforeLast;
					}
				}
			}
		}
		return s;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		String s = getString(columnIndex);
		return s == null ? null : new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		String s = getString(columnIndex);
		return s == null ? null : new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		byte[] bytes = getBytes(columnIndex);
		return bytes == null ? null : new ByteArrayInputStream(bytes);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		String s = getString(columnIndex);
		return s == null ? null : new StringReader(s);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(getColumnIndex(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(getColumnIndex(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(getColumnIndex(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(getColumnIndex(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(getColumnIndex(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(getColumnIndex(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(getColumnIndex(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(getColumnIndex(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(getColumnIndex(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(getColumnIndex(columnLabel), scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(getColumnIndex(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(getColumnIndex(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(getColumnIndex(columnLabel), cal);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(getColumnIndex(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(getColumnIndex(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(getColumnIndex(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return getTimestamp(getColumnIndex(columnLabel), cal);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(getColumnIndex(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(getColumnIndex(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(getColumnIndex(columnLabel));
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(getColumnIndex(columnLabel));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();
	}

	@Override
	public String getCursorName() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public HAResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		if (metadata == null) {
			metadata = new HAResultSetMetaData(this);
		}
		return metadata;
	}

	private Object getRawObject(int columnIndex) throws SQLException {
		Object[] currentRow = getCurrentRow();
		if (columnIndex < 1 || columnIndex > currentRow.length) {
			throw new HAException("Column index " + columnIndex + " is beyond range (1-" + currentRow.length + ")");
		}
		Object value = currentRow[columnIndex - 1];
		wasNull = (value == null);
		return value;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		Object value = getRawObject(columnIndex);
		if (value instanceof Instant) {
			return Timestamp.from((Instant) value);
		}
		return value;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		int index = getColumnIndex(columnLabel);
		return getObject(index);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if (type == null) {
			throw new HAException("Target type must not be null");
		}
		Object raw = getRawObject(columnIndex);
		if (raw == null) {
			return null;
		}
		if (type == Object.class) {
			return (T) getObject(columnIndex);
		}
		if (type.isInstance(raw)) {
			return type.cast(raw);
		}
		if (type == String.class) {
			return (T) getString(columnIndex);
		}
		if (type == Boolean.class) {
			return (T) Boolean.valueOf(getBoolean(columnIndex));
		}
		if (type == Integer.class) {
			return (T) Integer.valueOf(getInt(columnIndex));
		}
		if (type == Long.class) {
			return (T) Long.valueOf(getLong(columnIndex));
		}
		if (type == Short.class) {
			return (T) Short.valueOf(getShort(columnIndex));
		}
		if (type == Byte.class) {
			return (T) Byte.valueOf(getByte(columnIndex));
		}
		if (type == Float.class) {
			return (T) Float.valueOf(getFloat(columnIndex));
		}
		if (type == Double.class) {
			return (T) Double.valueOf(getDouble(columnIndex));
		}
		if (type == BigDecimal.class) {
			return (T) getBigDecimal(columnIndex);
		}
		if (type == byte[].class) {
			return (T) getBytes(columnIndex);
		}
		if (type == Date.class) {
			return (T) getDate(columnIndex);
		}
		if (type == Time.class) {
			return (T) getTime(columnIndex);
		}
		if (type == Timestamp.class) {
			return (T) getTimestamp(columnIndex);
		}
		if (type == Instant.class) {
			Timestamp ts = getTimestamp(columnIndex);
			return ts == null ? null : (T) ts.toInstant();
		}
		if (type == LocalDate.class) {
			Date d = getDate(columnIndex);
			return d == null ? null : (T) d.toLocalDate();
		}
		if (type == LocalTime.class) {
			Time t = getTime(columnIndex);
			return t == null ? null : (T) t.toLocalTime();
		}
		if (type == LocalDateTime.class) {
			Timestamp ts = getTimestamp(columnIndex);
			return ts == null ? null : (T) ts.toLocalDateTime();
		}
		if (type == OffsetDateTime.class) {
			Timestamp ts = getTimestamp(columnIndex);
			return ts == null ? null : (T) ts.toInstant().atZone(ZoneId.systemDefault()).toOffsetDateTime();
		}
		if (type == ZonedDateTime.class) {
			Timestamp ts = getTimestamp(columnIndex);
			return ts == null ? null : (T) ts.toInstant().atZone(ZoneId.systemDefault());
		}
		throw new HAException("Conversion to " + type.getName() + " is not supported");
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(getColumnIndex(columnLabel), type);
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return getColumnIndex(columnLabel);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

}
