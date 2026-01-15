# jdbc-ha

JDBC Driver for [SQLite HA](https://github.com/litesql/ha)

## Overview

`jdbc-ha` is a JDBC driver designed for high availability (HA) with SQLite databases. It provides seamless integration and enhanced performance for applications requiring reliable database connections.

## Features
- **High Availability**: Ensures continuous database access even during failures.
- **Easy Integration**: Simple setup with Maven for quick implementation.
- **Lightweight**: Minimal overhead for efficient performance.

## Usage

### 1. Add this to your `pom.xml`

```xml
<dependencies>
    <dependency>
        <groupId>io.github.litesql</groupId>
        <artifactId>jdbc-ha</artifactId>
        <version>1.0.9</version>
    </dependency>
</dependencies>

<repositories>
    <repository>
        <name>Buf Maven Repository</name>
        <id>buf</id>
        <url>https://maven.buf.build</url>
    </repository>
</repositories>
```

### 2. Configure the DataSource

To use the `com.github.litesql.jdbc.ha.HADataSource`, configure it in your application as follows:

```java
HADataSource dataSource = new HADataSource();
// Set properties as needed. Example:
datasource.setUrl("litesql://localhost:5001");
```

### 3. Example Usage

Here’s a simple example of how to use the driver:

```java
Connection connection = dataSource.getConnection();
// Perform database operations
```

## Load driver into DBeaver

[Download](https://github.com/litesql/jdbc-ha/releases) the full version jar and register as a custom driver to manage the database using [DBeaver](https://dbeaver.com/docs/dbeaver/Driver-Manager/#add-a-new-driver).

## Documentation

For detailed documentation, visit the [official documentation](https://github.com/litesql/ha).

## Contributing

Contributions are welcome! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the Apache v2 License - see the [LICENSE](LICENSE) file for details.
