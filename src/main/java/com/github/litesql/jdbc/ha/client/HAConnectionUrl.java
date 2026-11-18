package com.github.litesql.jdbc.ha.client;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import com.github.litesql.jdbc.ha.HAConstants;

/**
 * Parses LiteSQL HA connection URLs supporting single-host, comma-separated
 * multi-host clusters, IPv6 literals, and gRPC {@code dns://} targets.
 */
public final class HAConnectionUrl {

	public static final String MULTI_ENDPOINT_SCHEME = "litesql-cluster";

	public static final class Endpoint {
		private final String host;
		private final int port;
		private final boolean ipv6Literal;

		public Endpoint(String host, int port, boolean ipv6Literal) {
			this.host = Objects.requireNonNull(host, "host");
			this.port = port;
			this.ipv6Literal = ipv6Literal;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public boolean isIpv6Literal() {
			return ipv6Literal;
		}

		public String toHostPort() {
			if (ipv6Literal) {
				return "[" + host + "]:" + port;
			}
			return host + ":" + port;
		}

		public String toUriPathSegment() {
			if (ipv6Literal) {
				return "%5B" + host + "%5D:" + port;
			}
			return host + ":" + port;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof Endpoint)) {
				return false;
			}
			Endpoint that = (Endpoint) o;
			return port == that.port && ipv6Literal == that.ipv6Literal && host.equals(that.host);
		}

		@Override
		public int hashCode() {
			return Objects.hash(host, port, ipv6Literal);
		}

		@Override
		public String toString() {
			return toHostPort();
		}
	}

	private final String formattedUrl;
	private final String scheme;
	private final boolean plaintextByScheme;
	private final String dnsAuthority;
	private final List<Endpoint> endpoints;
	private final String replicationId;
	private final Map<String, String> queryParams;

	private HAConnectionUrl(String formattedUrl, String scheme, boolean plaintextByScheme, String dnsAuthority,
			List<Endpoint> endpoints, String replicationId, Map<String, String> queryParams) {
		this.formattedUrl = formattedUrl;
		this.scheme = scheme;
		this.plaintextByScheme = plaintextByScheme;
		this.dnsAuthority = dnsAuthority;
		this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
		this.replicationId = replicationId;
		this.queryParams = Collections.unmodifiableMap(new LinkedHashMap<>(queryParams));
	}

	public static HAConnectionUrl parse(String rawUrl) {
		if (rawUrl == null || rawUrl.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"Invalid connection URL: " + rawUrl + ".\nExpected URL formats: " + HAConstants.CONNECTION_URL_EXAMPLES);
		}

		String formatted = normalizeScheme(rawUrl.trim());

		String withoutFragment = formatted;
		int hashIdx = withoutFragment.indexOf('#');
		if (hashIdx >= 0) {
			withoutFragment = withoutFragment.substring(0, hashIdx);
		}

		String beforeQuery = withoutFragment;
		String queryString = null;
		int qIdx = withoutFragment.indexOf('?');
		if (qIdx >= 0) {
			beforeQuery = withoutFragment.substring(0, qIdx);
			queryString = withoutFragment.substring(qIdx + 1);
		}

		Map<String, String> queryParams = parseQueryString(queryString);

		String lowerBeforeQuery = beforeQuery.toLowerCase(Locale.ENGLISH);
		String scheme;
		String rest;
		if (lowerBeforeQuery.startsWith("http://")) {
			scheme = "http";
			rest = beforeQuery.substring("http://".length());
		} else if (lowerBeforeQuery.startsWith("https://")) {
			scheme = "https";
			rest = beforeQuery.substring("https://".length());
		} else if (lowerBeforeQuery.startsWith("dns://")) {
			scheme = "dns";
			rest = beforeQuery.substring("dns://".length());
		} else {
			throw new IllegalArgumentException(
					"Invalid connection URL: " + rawUrl + ".\nExpected URL formats: " + HAConstants.CONNECTION_URL_EXAMPLES);
		}

		boolean plaintextByScheme = "http".equals(scheme);
		boolean disableSslInQuery = Boolean.parseBoolean(queryParams.get(HAConstants.CONNECTION_PROPERTY_DISABLE_SSL));
		int defaultPort;
		if ("https".equals(scheme)) {
			defaultPort = 443;
		} else if ("http".equals(scheme)) {
			defaultPort = 80;
		} else {
			defaultPort = disableSslInQuery ? 80 : 443;
		}

		String dnsAuthority = "";
		String endpointsPart;
		String replicationId;

		if ("dns".equals(scheme)) {
			int firstSlash = rest.indexOf('/');
			if (firstSlash < 0) {
				endpointsPart = rest;
				replicationId = "";
			} else {
				dnsAuthority = rest.substring(0, firstSlash);
				String afterAuthority = rest.substring(firstSlash + 1);
				int secondSlash = afterAuthority.indexOf('/');
				if (secondSlash < 0) {
					endpointsPart = afterAuthority;
					replicationId = "";
				} else {
					endpointsPart = afterAuthority.substring(0, secondSlash);
					replicationId = afterAuthority.substring(secondSlash + 1);
				}
			}
		} else {
			int slashIdx = rest.indexOf('/');
			if (slashIdx < 0) {
				endpointsPart = rest;
				replicationId = "";
			} else {
				endpointsPart = rest.substring(0, slashIdx);
				replicationId = rest.substring(slashIdx + 1);
			}
			int atIdx = endpointsPart.lastIndexOf('@');
			if (atIdx >= 0) {
				endpointsPart = endpointsPart.substring(atIdx + 1);
			}
		}

		if (endpointsPart.trim().isEmpty()) {
			throw new IllegalArgumentException("Missing host in connection URL: " + rawUrl);
		}

		String[] rawEndpoints = endpointsPart.split(",", -1);
		List<Endpoint> endpoints = new ArrayList<>(rawEndpoints.length);
		for (String rawEp : rawEndpoints) {
			String trimmed = rawEp.trim();
			if (trimmed.isEmpty()) {
				throw new IllegalArgumentException("Empty host endpoint in connection URL: " + rawUrl);
			}
			endpoints.add(parseEndpoint(trimmed, defaultPort, rawUrl));
		}

		if (!dnsAuthority.isEmpty() && endpoints.size() > 1) {
			throw new IllegalArgumentException(
					"Custom DNS authority cannot be combined with multiple static endpoints in URL: " + rawUrl);
		}

		return new HAConnectionUrl(formatted, scheme, plaintextByScheme, dnsAuthority, endpoints, replicationId,
				queryParams);
	}

	private static String normalizeScheme(String url) {
		String formattedUrl = url;
		if (formattedUrl.startsWith("jdbc:litesql:ha:")) {
			formattedUrl = formattedUrl.substring("jdbc:litesql:ha:".length());
		}
		String lower = formattedUrl.toLowerCase(Locale.ENGLISH);
		if (lower.startsWith("litesql://")) {
			formattedUrl = "http://" + formattedUrl.substring("litesql://".length());
		} else if (lower.startsWith("litesqls://")) {
			formattedUrl = "https://" + formattedUrl.substring("litesqls://".length());
		} else if (formattedUrl.startsWith("//")) {
			formattedUrl = "http:" + formattedUrl;
		}
		return formattedUrl;
	}

	public static Endpoint parseEndpoint(String endpointStr, int defaultPort, String contextUrl) {
		String host;
		int port;
		boolean ipv6 = false;

		if (endpointStr.startsWith("[")) {
			int closeIdx = endpointStr.indexOf(']');
			if (closeIdx <= 1) {
				throw new IllegalArgumentException("Invalid IPv6 host in connection URL: " + contextUrl);
			}
			host = endpointStr.substring(1, closeIdx);
			ipv6 = true;
			String remainder = endpointStr.substring(closeIdx + 1);
			if (remainder.isEmpty()) {
				port = defaultPort;
			} else if (remainder.startsWith(":")) {
				port = parsePort(remainder.substring(1), contextUrl);
			} else {
				throw new IllegalArgumentException("Invalid endpoint '" + endpointStr + "' in URL: " + contextUrl);
			}
		} else {
			int firstColon = endpointStr.indexOf(':');
			int lastColon = endpointStr.lastIndexOf(':');
			if (firstColon != lastColon) {
				throw new IllegalArgumentException(
						"IPv6 address must be enclosed in brackets '[...]' in connection URL: " + contextUrl);
			}
			if (firstColon < 0) {
				host = endpointStr;
				port = defaultPort;
			} else {
				host = endpointStr.substring(0, firstColon);
				port = parsePort(endpointStr.substring(firstColon + 1), contextUrl);
			}
		}

		if (host.isEmpty() || containsInvalidHostChars(host, ipv6)) {
			throw new IllegalArgumentException("Invalid host '" + host + "' in connection URL: " + contextUrl);
		}

		return new Endpoint(host, port, ipv6);
	}

	private static boolean containsInvalidHostChars(String host, boolean ipv6) {
		for (int i = 0; i < host.length(); i++) {
			char c = host.charAt(i);
			if (Character.isWhitespace(c) || c == '/' || c == '\\' || c == '?' || c == '#' || c == '[' || c == ']') {
				return true;
			}
			if (!ipv6 && c == ':') {
				return true;
			}
		}
		return false;
	}

	private static int parsePort(String portStr, String contextUrl) {
		if (portStr.isEmpty()) {
			throw new IllegalArgumentException("Empty port in connection URL: " + contextUrl);
		}
		try {
			int port = Integer.parseInt(portStr);
			if (port <= 0 || port > 65535) {
				throw new IllegalArgumentException("Port out of range (" + port + ") in connection URL: " + contextUrl);
			}
			return port;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid port '" + portStr + "' in connection URL: " + contextUrl, e);
		}
	}

	public static Map<String, String> parseQueryString(String queryString) {
		Map<String, String> queryParams = new LinkedHashMap<>();
		if (queryString != null && !queryString.isEmpty()) {
			String[] params = queryString.split("&");
			for (String param : params) {
				if (param.isEmpty()) {
					continue;
				}
				int eqIdx = param.indexOf('=');
				if (eqIdx > 0) {
					String key = URLDecoder.decode(param.substring(0, eqIdx), StandardCharsets.UTF_8);
					String value = URLDecoder.decode(param.substring(eqIdx + 1), StandardCharsets.UTF_8);
					queryParams.put(key, value);
				} else if (eqIdx < 0) {
					String key = URLDecoder.decode(param, StandardCharsets.UTF_8);
					queryParams.put(key, "");
				}
			}
		}
		return queryParams;
	}

	public String getFormattedUrl() {
		return formattedUrl;
	}

	public String getScheme() {
		return scheme;
	}

	public boolean isPlaintextByScheme() {
		return plaintextByScheme;
	}

	public String getDnsAuthority() {
		return dnsAuthority;
	}

	public List<Endpoint> getEndpoints() {
		return endpoints;
	}

	public String getReplicationId() {
		return replicationId;
	}

	public Map<String, String> getQueryParams() {
		return queryParams;
	}

	public String toGrpcTarget() {
		if (endpoints.size() == 1) {
			return "dns://" + dnsAuthority + "/" + endpoints.get(0).toUriPathSegment();
		}
		StringBuilder sb = new StringBuilder(MULTI_ENDPOINT_SCHEME).append(":///");
		for (int i = 0; i < endpoints.size(); i++) {
			if (i > 0) {
				sb.append(',');
			}
			sb.append(endpoints.get(i).toUriPathSegment());
		}
		return sb.toString();
	}
}
