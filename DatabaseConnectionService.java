import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnectionService {

    public static void main(String[] args) {
        DatabaseConnectionService dbService = new DatabaseConnectionService();
        // Try to connect using Environment Variables first
        if (dbService.connect()) {
            System.out.println("Connected to the database successfully.");
            dbService.closeConnection();
        } else {
            System.out.println("Failed to connect to the database.");
        }
        
        DataImport di = new DataImport(dbService);

        String[] tables = {
        "Person",               // Base table for Actor/Staff
        "Company",              // Base for Equipment/Prop
        "Location",             // Base for Scene
        "Film",                 // Base for Scene
        "Storage",              // Base for Equipment/Prop
        "Actor",                // Depends on Person
        "Staff",                // Depends on Person
        "Availability",         // Depends on Person
        "StaffSupportsActor",   // Depends on Staff and Actor
        "Scene",                // Depends on Film and Location
        "Equipment",            // Depends on Storage and Company
        "Prop",                 // Depends on Storage and Company
        "SceneEquipment",       // Depends on Scene and Equipment
        "SceneProps",           // Depends on Scene and Prop
        "SceneNeedsStaffType",  // Depends on Scene
        "Acts"                  // Depends on Actor, Scene, and Prop
    };

    for (String table : tables) {
        System.out.println("Inserting data for: " + table);
        boolean success = DataImport.addToDatabase(table.toLowerCase());
        
        if (success) {
            System.out.println("Successfully populated " + table);
        } else {
            System.err.println("FAILED to populate " + table + ". Stopping execution.");
            break; // Stop to prevent massive FK error cascades
        }
    }

    }

    private final String url = "jdbc:sqlserver://${dbServer};databaseName=${dbName};user=${user};password=${pass};encrypt=false;";

    private Connection connection = null;
    private String databaseName = "GLUTest";
    private String serverName = "golem.csse.rose-hulman.edu";

    public DatabaseConnectionService() {
    }

    public boolean connect() {
        // Use Spring-injected properties if available, otherwise try Environment Variables
        String userToUse = password.user;
        String passToUse = password.password;

        if (userToUse != null && passToUse != null) {
            return connect(userToUse, passToUse);
        } else {
            return connect("PLACEHOLDER_USER", "PLACEHOLDER_PASS");
        }
    }

    public boolean connect(String user, String pass) {
        try {
            String fullUrl = url
                .replace("${dbServer}", this.serverName)
                .replace("${dbName}", this.databaseName)
                .replace("${user}", user)
                .replace("${pass}", pass);

            this.connection = DriverManager.getConnection(fullUrl);
            return true;
        } catch (SQLException e) {
            System.err.println("Database connection failed for user: " + user);
            e.printStackTrace();
            this.connection = null;
            return false;
        }
    }

    public Connection getConnection() {
        try {
            if (this.connection == null || this.connection.isClosed()) {
                this.connect();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return this.connection;
    }

    public void closeConnection() {
        try {
            if (this.connection != null && !this.connection.isClosed()) {
                this.connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
