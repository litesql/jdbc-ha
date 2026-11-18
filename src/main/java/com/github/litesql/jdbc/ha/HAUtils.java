package com.github.litesql.jdbc.ha;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jkiss.code.NotNull;

import com.github.litesql.jdbc.ha.client.HAConnectionUrl;

public class HAUtils {

	private static final Pattern DML_KEYWORD_PATTERN = Pattern
			.compile("\\b(insert|update|delete|replace)\\b", Pattern.CASE_INSENSITIVE);

	public static String quote(String literal) {
		if (literal == null) {
			return "NULL";
		}
		return "'" + escape(literal) + "'";
	}

	/**
	 * Follow rules in <a href="https://www.sqlite.org/lang_keywords.html">SQLite
	 * Keywords</a>
	 *
	 * @param name Identifier name
	 * @return Unquoted identifier
	 */
	public static String unquote(String name) {
		if (name == null) {
			return null;
		}
		name = name.trim();
		if (name.length() >= 2) {
			if (name.startsWith("`") && name.endsWith("`")) {
				return name.substring(1, name.length() - 1).replace("``", "`");
			}
			if (name.startsWith("\"") && name.endsWith("\"")) {
				return name.substring(1, name.length() - 1).replace("\"\"", "\"");
			}
			if (name.startsWith("[") && name.endsWith("]")) {
				return name.substring(1, name.length() - 1);
			}
		}
		return name;
	}

	public static String escape(final String val) {
		if (val == null) {
			return null;
		}
		if (val.indexOf('\'') == -1) {
			return val;
		}
		int len = val.length();
		StringBuilder buf = new StringBuilder(len + 4);
		for (int i = 0; i < len; i++) {
			char ch = val.charAt(i);
			if (ch == '\'') {
				buf.append('\'');
			}
			buf.append(ch);
		}
		return buf.toString();
	}

	public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
		Statement stat = connection.createStatement();
		try {
			stat.closeOnCompletion();
			return stat.executeQuery(query);
		} catch (Throwable t) {
			try {
				stat.close();
			} catch (SQLException ignored) {
			}
			throw t;
		}
	}

	@NotNull
	public static String validateAndFormatUrl(String url) throws HAException {
		if (url == null) {
			throw new HAException("Invalid connection URL: null.\nExpected URL formats: "
					+ HAConstants.CONNECTION_URL_EXAMPLES);
		}
		Matcher matcher = HAConstants.CONNECTION_URL_PATTERN.matcher(url);
		if (!matcher.matches()) {
			throw new HAException("Invalid connection URL: " + url + ".\nExpected URL formats: "
					+ HAConstants.CONNECTION_URL_EXAMPLES);
		}

		try {
			return HAConnectionUrl.parse(url).getFormattedUrl();
		} catch (IllegalArgumentException e) {
			throw new HAException(e.getMessage(), e);
		}
	}

	public static String stripLeadingComments(String sql) {
		if (sql == null) {
			return "";
		}
		int i = 0;
		int len = sql.length();
		while (i < len) {
			while (i < len && Character.isWhitespace(sql.charAt(i))) {
				i++;
			}
			if (i + 1 < len && sql.charAt(i) == '-' && sql.charAt(i + 1) == '-') {
				i += 2;
				while (i < len && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
					i++;
				}
			} else if (i + 1 < len && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
				i += 2;
				while (i + 1 < len && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
					i++;
				}
				if (i + 1 < len) {
					i += 2;
				} else {
					return "";
				}
			} else {
				break;
			}
		}
		return sql.substring(i);
	}

	public static boolean isSelectQuery(String sql) {
		if (sql == null) {
			return false;
		}
		String stripped = stripLeadingComments(sql).toLowerCase(Locale.ENGLISH);
		if (stripped.startsWith("select") || stripped.startsWith("values")) {
			return true;
		}
		if (stripped.startsWith("with")) {
			return !DML_KEYWORD_PATTERN.matcher(stripped).find();
		}
		return false;
	}

	public static boolean isTransactionControlQuery(String sql) {
		if (sql == null) {
			return false;
		}
		String stripped = stripLeadingComments(sql).toLowerCase(Locale.ENGLISH);
		return stripped.startsWith("begin") || stripped.startsWith("commit") || stripped.startsWith("end")
				|| stripped.startsWith("rollback");
	}

	public static Path resolveSafePath(String dir, String replicaID) throws IOException {
		if (dir == null || dir.isEmpty()) {
			throw new IOException("Target directory must not be empty");
		}
		if (replicaID == null || replicaID.isEmpty()) {
			throw new IOException("Replication ID must not be empty");
		}
		Path baseDir = Paths.get(dir).toAbsolutePath().normalize();
		Path resolved = baseDir.resolve(replicaID).normalize();
		if (!resolved.startsWith(baseDir)) {
			throw new IOException("Invalid replication ID path: " + replicaID);
		}
		return resolved;
	}

	public static long writeFileChunk(String dir, String replicaID, byte[] data, long offset) {
		try {
			Path path = resolveSafePath(dir, replicaID);
			Path parent = path.getParent();
			if (parent != null) {
				Files.createDirectories(parent);
			}
			try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
				if (offset == 0) {
					raf.setLength(0);
				}
				raf.seek(offset);
				raf.write(data);
			}
			return offset + data.length;
		} catch (IOException e) {
			throw new RuntimeException("Failed to write replica file chunk for " + replicaID, e);
		}
	}

}
