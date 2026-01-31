package com.github.litesql.jdbc.ha;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;

import org.jkiss.code.NotNull;

public class HAUtils {

	public static String quote(String identifier) {
		return "'" + identifier + "'";
	}

	/**
	 * Follow rules in <a href="https://www.sqlite.org/lang_keywords.html">SQLite
	 * Keywords</a>
	 *
	 * @param name Identifier name
	 * @return Unquoted identifier
	 */
	public static String unquote(String name) {
		if (name == null)
			return name;
		name = name.trim();
		if (name.length() > 2 && ((name.startsWith("`") && name.endsWith("`"))
				|| (name.startsWith("\"") && name.endsWith("\"")) || (name.startsWith("[") && name.endsWith("]")))) {
			// unquote to be consistent with column names returned by getColumns()
			name = name.substring(1, name.length() - 1);
		}
		return name;
	}

	public static String escape(final String val) {
		if (val.indexOf('\'') == 1) {
			return val;
		}
		int len = val.length();
		StringBuilder buf = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			if (val.charAt(i) == '\'') {
				buf.append('\'');
			}
			buf.append(val.charAt(i));
		}
		return buf.toString();
	}

	public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
		try (Statement stat = connection.createStatement()) {
			return stat.executeQuery(query);
		}
	}

	@NotNull
	public static String validateAndFormatUrl(@NotNull String url) throws HAException {
		Matcher matcher = HAConstants.CONNECTION_URL_PATTERN.matcher(url);
		if (!matcher.matches()) {
			throw new HAException("Invalid connection URL: " + url + ".\nExpected URL formats: "
					+ HAConstants.CONNECTION_URL_EXAMPLES);
		}

		String formattedUrl = url;
		if (formattedUrl.startsWith("jdbc:litesql:ha:")) {
			formattedUrl = formattedUrl.replaceFirst("jdbc:litesql:ha:", "");
		}
		if (formattedUrl.startsWith("litesql://")) {
			formattedUrl = formattedUrl.replaceFirst("litesql://", "http://");
		}
		return formattedUrl;
	}

	public static boolean isSelectQuery(String sql) {
		String trimmedSql = sql.trim().toLowerCase();
		return trimmedSql.startsWith("select") || trimmedSql.startsWith("with");
	}

	public static boolean isTransactionControlQuery(String sql) {
		String trimmedSql = sql.trim().toLowerCase();
		return trimmedSql.startsWith("begin") || trimmedSql.startsWith("commit") || trimmedSql.startsWith("rollback");
	}

	public static long writeFileChunk(String dir, String replicaID, byte[] data, long offset) {
		try {
			java.nio.file.Path path = java.nio.file.Paths.get(dir, replicaID);
			java.nio.file.Files.createDirectories(path.getParent());
			try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path.toFile(), "rw")) {
				raf.seek(offset);
				raf.write(data);
			}
			return offset + data.length;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
