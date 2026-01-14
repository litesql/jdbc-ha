# jdbc-ha
JDBC driver for [SQLite HA](https://github.com/litesql/ha)

## Usage

### 1. Add this to your pom.xml

```xml
<dependencies>
    <dependency>
        <groupId>io.github.litesql</groupId>
        <artifactId>jdbc-ha</artifactId>
        <version>1.0.5</version>
    </dependency>
    <dependency>
        <groupId>com.dbeaver.common</groupId>
        <artifactId>com.dbeaver.jdbc.api</artifactId>
        <version>2.4.0</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-netty-shaded</artifactId>
        <version>1.78.0</version>
    </dependency>
    <dependency>
        <groupId>build.buf.gen</groupId>
        <artifactId>litesql_sqlite-ha_grpc_java</artifactId>
        <version>1.78.0.1.20260110202014.9b378ae49587</version>
    </dependency>
</dependencies>
	
<repositories>
    <repository>
        <name>Buf Maven Repository</name>
        <id>buf</id>
        <url>https://buf.build/gen/maven</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
</repositories>
```

### 2. Example:

```java
import java.sql.*;

public class Example {
    
    public static void main(String[] args) {
        String serverURL = "jdbc:litesql:ha:http://localhost:5001/chinook.db";
        Connection connection = DriverManager.getConnection(serverURL, null, null));
        // Use connection...
        
    }
}
```