import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.io.File;
import java.io.IOException;

/**
 * Performs SQL DDL and SELECT queries on a MySQL database hosted on AWS RDS.
 */
public class AmazonRedshift {
    /**
     * Connection to database
     */
    private Connection con;
    /**
     * TODO: Fill in AWS connection information.
     */
    private String url = "jdbc:redshift://redshift-cluster-g23ai2028.c2qwhzzynm4f.us-east-1.redshift.amazonaws.com:5439/dev";
    private String uid = "g23ai2028";
    private String pw = "Shubhamg23ai2028";

    /**
     * Main method is only used for convenience. Use JUnit test file to verify your
     * answer.
     *
     * @param args none expected
     * @throws SQLException if a database error occurs
     */
    public static void main(String[] args) throws SQLException {
        AmazonRedshift q = new AmazonRedshift();
        q.connect();
        q.drop();
        q.create();
        q.insert();
        q.query1();
        q.query2();
        q.query3();
        q.close();
    }

    /**
     * Makes a connection to the database and returns connection to caller.
     *
     * @return connection
     * @throws SQLException if an error occurs
     */
    public Connection connect() throws SQLException {
        // TODO: For connect to work you must configure your AWS connection info in the
        // private instance variables at the top of the file.
        // If connection fails, make sure to modify your VPC rules to along inbound
        // traffic to the database from your IP.
        System.out.println("Connecting to database of redshift.");
        con = DriverManager.getConnection(url, uid, pw);
        System.out.println("Connected AWS redShift database successfully.");
        return con;
    }

    /**
     * Closes connection to database.
     */
    public void close() {
        if (con != null) {
            try {
                con.close();
                System.out.println("Database connection AWS redShift closed successfully.");
            } catch (SQLException e) {
                System.err.println("Error while closing the database connection: " + e.getMessage());
            }
        } else {
            System.out.println("No open connection to close since all is cloed.");
        }
    }

    /**
     * Drops all the tables from the database schema `dev`.
     */
    public void drop() {
        System.out.println("Dropping all the tables.");
        String queryFetchTables = "SELECT tablename FROM pg_tables WHERE schemaname = 'dev';";

        try (Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(queryFetchTables)) {

            // Iterating through all the tables in the schema `dev`
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                String dropQuery = "DROP TABLE IF EXISTS dev." + tableName + " CASCADE;";
                try (Statement dropStmt = con.createStatement()) {
                    dropStmt.executeUpdate(dropQuery);
                    System.out.println("Dropped table: " + tableName);
                }
            }
            System.out.println("All tables dropped successfully.");

        } catch (SQLException e) {
            System.err.println("Error while dropping tables: " + e.getMessage());
        }
    }

    /**
     * Creates the schema `dev` and the required tables in the database.
     */
    public void create() throws SQLException {
        System.out.println("Creating schema and tables.");

        // Step 1: Creating Schema `dev` if not exists
        String createSchemaQuery = "CREATE SCHEMA IF NOT EXISTS dev;";
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createSchemaQuery);
            System.out.println("Schema `dev` created or already exists.");
        }

        // Step 2: Createing Tables in `dev` schema
        String createTablesQuery = """
                    CREATE TABLE IF NOT EXISTS dev.orders (
                        order_id INT PRIMARY KEY,
                        customer_id INT NOT NULL,
                        order_date DATE NOT NULL,
                        total_amount DECIMAL(10, 2) NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS dev.customers (
                        customer_id INT PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        country VARCHAR(100) NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS dev.line_items (
                        line_item_id INT PRIMARY KEY,
                        order_id INT NOT NULL,
                        product_id INT NOT NULL,
                        quantity INT NOT NULL,
                        price DECIMAL(10, 2) NOT NULL,
                        FOREIGN KEY (order_id) REFERENCES dev.orders(order_id)
                    );
                """;

        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createTablesQuery);
            System.out.println("Tables created successfully in schema `dev`.");
        } catch (SQLException e) {
            System.err.println("Error creating tables: " + e.getMessage());
            throw e;
        }
    }

    public void insert() throws SQLException {
        System.out.println("Loading TPC-H Data");
    
        // adding the path to the folder containing SQL files
        String ddlFolderPath = "ddl_data/";
    
        // List of SQL files to execute (update based on your folder structure)
        String[] sqlFiles = {
            "tpch_create.sql",      // Creating all necessary tabless
            "customer.sql",         // Inserting all data into customer table
            "lineitem.sql",         // Inserting all data into line item table
            "nation.sql",           // Inserting all data into nation table
            "orders.sql",           // Inserting all data into orders table
            "part.sql",             // Inserting all data into part table
            "partsupp.sql",         // Inserting all data into part supplier table
            "region.sql",           // Inserting all data into region table
            "supplier.sql"          // Inserting all data into supplier table
        };
    
        try {
            for (String sqlFile : sqlFiles) {
                String filePath = ddlFolderPath + sqlFile;
    
                // Reading the file contents
                String sql = new String(Files.readAllBytes(Paths.get(filePath)));
    
                // Spliting the SQL into individual statements
                String[] commands = sql.split(";");
    
                try (Statement stmt = con.createStatement()) {
                    int count = 0;
    
                    // Add each command to the batch
                    for (String command : commands) {
                        if (!command.trim().isEmpty()) {
                            stmt.addBatch(command.trim());
                            count++;
    
                            // Execute batch after a certain number of commands
                            if (count % 500 == 0) { // Adjust batch size as per my compute needed
                                stmt.executeBatch();
                                System.out.println("Executed 500 commands from file: " + sqlFile);
                            }
                        }
                    }
    
                    // Executing remaining commands
                    stmt.executeBatch();
                    System.out.println("Executed all commands from file: " + sqlFile);
                } catch (SQLException e) {
                    System.err.println("Error executing batch from " + sqlFile + ": " + e.getMessage());
                }
            }
            System.out.println("TPC-H Data loaded successfully.");
        } catch (IOException e) {
            System.err.println("Error reading SQL files: " + e.getMessage());
        }
    }
    

    /**
     * Query returns the most recent top 10 orders with the total sale and the date
     * of the order in `America`.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query1() throws SQLException {
        System.out.println("Executing query #1.");
    
        // SQL query to fetch the required data
        String sql = """
            SELECT o.order_id, o.total_amount, o.order_date
            FROM dev.orders o
            JOIN dev.customers c ON o.customer_id = c.customer_id
            WHERE c.country = 'America'
            ORDER BY o.order_date DESC
            LIMIT 10;
        """;
    
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("Query #1 executed successfully.");
            return rs; // Returning the result set to the caller
        } catch (SQLException e) {
            System.err.println("Error executing query #1: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Query returns the customer key and the total price a customer spent in
     * descending order, for all urgent orders that are not failed for all customers
     * who are outside Europe belonging to the highest market segment.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query2() throws SQLException {
        System.out.println("Executing query #2.");
    
        // SQL query to fetch the required data
        String sql = """
            WITH LargestMarketSegment AS (
                SELECT c.market_segment
                FROM dev.customers c
                GROUP BY c.market_segment
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            SELECT c.customer_id, SUM(o.total_amount) AS total_spent
            FROM dev.customers c
            JOIN dev.orders o ON c.customer_id = o.customer_id
            WHERE c.country NOT IN ('Europe')
              AND o.order_priority = 'URGENT'
              AND o.status != 'FAILED'
              AND c.market_segment = (SELECT market_segment FROM LargestMarketSegment)
            GROUP BY c.customer_id
            ORDER BY total_spent DESC;
        """;
    
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("Query #2 executed successfully.");
            return rs; // Returning the result set to the caller
        } catch (SQLException e) {
            System.err.println("Error executing query #2: " + e.getMessage());
            throw e;
        }
    }

        /**
         * Query returns all the lineitems that was ordered within the six years from
         * January 4th, 1997 and the orderpriority in ascending order.
         *
         * @return ResultSet
         * @throws SQLException if an error occurs
         */
        public ResultSet query3() throws SQLException {
            System.out.println("Executing query #3.");
        
            // SQL query to fetch the required data
            String sql = """
                SELECT o.order_priority, COUNT(li.line_item_id) AS line_item_count
                FROM dev.orders o
                JOIN dev.line_items li ON o.order_id = li.order_id
                WHERE o.order_date >= '1997-04-01'
                AND o.order_date < '2003-04-01'
                GROUP BY o.order_priority
                ORDER BY o.order_priority ASC;
            """;
        
            try (Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                System.out.println("Query #3 executed successfully.");
                return rs; // Returning the result set to the caller
            } catch (SQLException e) {
                System.err.println("Error executing query #3: " + e.getMessage());
                throw e;
            }
        }

    /*
     * Do not change anything below here.
     */

    /**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined but only the first few are put into a string.
     *
     * @param rst     ResultSet
     * @param maxrows maximum number of rows to display
     * @return String form of results
     * @throws SQLException if a database error occurs
     */
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    /**
     * Converts ResultSetMetaData into a string.
     *
     * @param meta ResultSetMetaData
     * @return string form of metadata
     * @throws SQLException if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + ", "
                + meta.getColumnType(1) + "-" + meta.getColumnTypeName(1) + ", "
                + meta.getColumnDisplaySize(1) + ", " + meta.getPrecision(1) + ", " + meta.getScale(1) + ")");
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + ", "
                    + meta.getColumnType(j) + "-" + meta.getColumnTypeName(j) + ", "
                    + meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j) + ", " + meta.getScale(j) + ")");
        return buf.toString();
    }
}
