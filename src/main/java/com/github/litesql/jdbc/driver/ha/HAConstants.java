/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2025 DBeaver Corp and others
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
package com.github.litesql.jdbc.driver.ha;

import java.util.regex.Pattern;

public class HAConstants {

    public static final String CONNECTION_URL_EXAMPLES = "jdbc:litesql:ha:<server-url>, litesql://";
    public static final Pattern CONNECTION_URL_PATTERN = Pattern.compile("(jdbc:litesql:ha:|litesql://)(.+)");

    public static final int DRIVER_VERSION_MAJOR = 1;
    public static final int DRIVER_VERSION_MINOR = 0;
    public static final int DRIVER_VERSION_MICRO = 6;

    public static final String DRIVER_NAME = "LiteSQL HA";
    public static final String DRIVER_INFO = "LiteSQL HA JDBC driver";

    public static final String DEFAULT_ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
}
