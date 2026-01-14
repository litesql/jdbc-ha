# jdbc-ha
JDBC driver for [SQLite HA](https://github.com/litesql/ha)

## Usage

### 1. Add this to your pom.xml

```xml
<dependencies>
    <dependency>
        <groupId>io.github.litesql</groupId>
        <artifactId>jdbc-ha</artifactId>
        <version>1.0.6</version>
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
        String serverURL = "litesql://localhost:5001";
        Connection connection = DriverManager.getConnection(serverURL, null, null));
        // Use connection...
        
    }
}
```