/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.litesql.jdbc.ha.client;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;

public class HAReaderInput {
	private final Reader stream;
	private final long length;
	private String cachedValue;
	private boolean loaded;

	public HAReaderInput(Reader stream, long length) {
		this.stream = stream;
		this.length = length;
	}

	public synchronized String readString() throws IOException {
		if (loaded) {
			return cachedValue;
		}
		if (stream == null) {
			loaded = true;
			cachedValue = null;
			return null;
		}
		StringBuilder sb = new StringBuilder();
		char[] buffer = new char[4096];
		if (length >= 0) {
			long remaining = length;
			while (remaining > 0) {
				int toRead = (int) Math.min(buffer.length, remaining);
				int read = stream.read(buffer, 0, toRead);
				if (read == -1) {
					break;
				}
				sb.append(buffer, 0, read);
				remaining -= read;
			}
		} else {
			int read;
			while ((read = stream.read(buffer)) != -1) {
				sb.append(buffer, 0, read);
			}
		}
		cachedValue = sb.toString();
		loaded = true;
		return cachedValue;
	}

	@Override
	public String toString() {
		try {
			String s = readString();
			return s == null ? "" : s;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
