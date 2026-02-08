import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.io.*;
import java.math.BigDecimal;

public class DataImport {

    private static DatabaseConnectionService dbService = null;

	public DataImport(DatabaseConnectionService dbService) {
		DataImport.dbService = dbService;
	}

    public static boolean addToDatabase(String table){

        CallableStatement cs = null;
        ArrayList<genericData> agd= getData(table);
        for(int i = 0; i<agd.size(); i++){
            try {
                genericData gd = agd.get(i);
                cs = prepareCall(table);
                setValues(cs, table, gd);

                int rows = cs.executeUpdate();
                if(rows == 0) System.out.print(i + table + ",");
                //return true;
            } catch (SQLException ex) {
                System.err.println("Failed to insert into " + table);
                System.err.println("SQL State: " + ex.getSQLState());
                System.err.println("Error Code: " + ex.getErrorCode());
                System.err.println("Message: " + ex.getMessage());
                ex.printStackTrace();
                return false;
            } finally {
                try {
                    if (cs != null) cs.close();
                }
                catch (SQLException e) {
                    return false;
                }
            }
        }
        return true;
    }

    public static ArrayList<genericData> getData(String table) {
        ArrayList<genericData> dataList = new ArrayList<>();
        // Files are named tableName.csv (e.g., Person.csv)
        String fileName = table + ".csv";
        
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String headerLine = br.readLine();

            // Split the first row to get the datatypes (e.g., "int", "string", "int")
            String[] dataTypes = headerLine.split(",");
            String line;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                genericData gd = new genericData();

                // Loop through each column based on the datatypes defined in the first row
                for (int i = 0; i < dataTypes.length; i++) {
                    String type = dataTypes[i];
                    String val =  values[i];
                    //System.out.print(values[i] + ",");
                    if (type.equals("int")) {
                        int intVal = Integer.parseInt(val);
                        gd.addInts(intVal);
                    } else if (type.equals("string")) {
                        //If the string is empty it is automatically processed as null by SPROCS
                        gd.addStrings(val);                        
                    }
                }
                //System.out.println();
                dataList.add(gd);
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error: The file " + fileName + " DNE.");
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Error: non int in int column");
        }

        return dataList;
    }

    public static CallableStatement prepareCall(String table){
        try{
        switch(table) {
            case "person":
                // SSN, Phone, FName, MI, LName
                return dbService.getConnection().prepareCall("{call InsertPerson(?, ?, ?, ?, ?)}");
            case "actor":
                // PersonID, Part
                return dbService.getConnection().prepareCall("{call InsertActor(?, ?)}");
            case "staff":
                // PersonID, Type
                return dbService.getConnection().prepareCall("{call InsertStaff(?, ?)}");
            case "availability":
                // PersonID, StartTime, EndTime
                return dbService.getConnection().prepareCall("{call InsertAvailability(?, ?, ?)}");
            case "staffsupportsactor":
                // ActorID, StaffID
                return dbService.getConnection().prepareCall("{call InsertStaffSupportsActor(?, ?)}");
            case "company":
                // Name, Phone
                return dbService.getConnection().prepareCall("{call InsertCompany(?, ?)}");
            case "film":
                // Title, ReleaseDate, Budget, Studio
                return dbService.getConnection().prepareCall("{call InsertFilm(?, ?, ?, ?)}");
            case "location":
                // Latitude, Longitude
                return dbService.getConnection().prepareCall("{call InsertLocation(?, ?)}");
            case "scene":
                // SceneNumber, TimeOfDay, Desc, FilmID, LocationID
                return dbService.getConnection().prepareCall("{call InsertScene(?, ?, ?, ?, ?)}");
            case "sceneneedsstafftype":
                // SceneID, Type
                return dbService.getConnection().prepareCall("{call InsertSceneNeedsStaffType(?, ?)}");
            case "storage":
                // Country, City, StateAbbv, Zip, StreetAddr, Shelf
                return dbService.getConnection().prepareCall("{call InsertStorage(?, ?, ?, ?, ?, ?)}");
            case "equipment":
                // Type, Name, IsRented, StorageID, CompanyID
                return dbService.getConnection().prepareCall("{call InsertEquipment(?, ?, ?, ?, ?)}");
            case "prop":
                // Type, Name, IsRented, StorageID, CompanyID
                return dbService.getConnection().prepareCall("{call InsertProp(?, ?, ?, ?, ?)}");
            case "sceneequipment":
                // SceneID, EquipmentID
                return dbService.getConnection().prepareCall("{call InsertSceneEquipment(?, ?)}");
            case "sceneprops":
                // SceneID, PropID
                return dbService.getConnection().prepareCall("{call InsertSceneProps(?, ?)}");
            case "acts":
                // ActorID, SceneID, PropID
                return dbService.getConnection().prepareCall("{call InsertActs(?, ?, ?)}");
            default:
                return null;
        }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static void setValues(CallableStatement cs, String table, genericData gd){
        try{
        switch(table) {
            case "person":
                // SSN, Phone, FName, MI, LName
                //System.out.println(gd.strings.get(2));
                cs.setString(1, gd.strings.remove(0));
                cs.setString(2, gd.strings.remove(0));
                cs.setString(3, gd.strings.remove(0));
                cs.setString(4, gd.strings.remove(0));
                cs.setString(5, gd.strings.remove(0));
                break;

            case "actor":
                // PersonID, Part
                cs.setInt(1, gd.ints.remove(0));
                cs.setString(2, gd.strings.remove(0));
                break;

            case "staff":
                // PersonID, Type
                cs.setInt(1, gd.ints.remove(0));
                cs.setString(2, gd.strings.remove(0));
                break;

            case "availability":
                // PersonID, StartTime (Date as String), EndTime (Date as String)
                cs.setInt(1, gd.ints.remove(0));
                cs.setString(2, gd.strings.remove(0));
                if(!gd.strings.get(0).equals("NULL")) cs.setString(3, gd.strings.remove(0));
                else cs.setString(3, null);
                break;

            case "staffsupportsactor":
                // ActorID, StaffID
                cs.setInt(1, gd.ints.remove(0));
                cs.setInt(2, gd.ints.remove(0));
                break;

            case "company":
                // Name, Phone
                cs.setString(1, gd.strings.remove(0));
                cs.setString(2, gd.strings.remove(0));
                break;

            case "film":
                // Title, ReleaseDate (String), Budget, Studio
                cs.setString(1, gd.strings.remove(0));
                cs.setString(2, gd.strings.remove(0));
                cs.setBigDecimal(3, new BigDecimal(gd.strings.remove(0)));
                cs.setString(4, gd.strings.remove(0));
                break;

            case "location":
                // Latitude (Decimal as String), Longitude (Decimal as String)
                cs.setBigDecimal(1, new BigDecimal(gd.strings.remove(0)));
                cs.setBigDecimal(2, new BigDecimal(gd.strings.remove(0)));
                break;

            case "scene":
                // SceneNumber, TimeOfDay, Desc, FilmID, LocationID
                cs.setInt(1, gd.ints.remove(0));
                cs.setString(2, gd.strings.remove(0));
                cs.setString(3, gd.strings.remove(0));
                cs.setInt(4, gd.ints.remove(0));
                cs.setInt(5, gd.ints.remove(0));
                break;

            case "sceneneedsstafftype":
                // SceneID, Type
                cs.setInt(1, gd.ints.remove(0));
                cs.setString(2, gd.strings.remove(0));
                break;

            case "storage":
                // Country, City, StateAbbv, Zip, StreetAddr, Shelf
                cs.setString(1, gd.strings.remove(0));
                cs.setString(2, gd.strings.remove(0));
                cs.setString(3, gd.strings.remove(0));
                cs.setString(4, gd.strings.remove(0));
                cs.setString(5, gd.strings.remove(0));
                cs.setInt(6, gd.ints.remove(0));
                break;

            case "equipment":
            case "prop":
                // Type, Name, IsRented (bit as Int), StorageID, CompanyID
                cs.setString(1, gd.strings.remove(0));
                cs.setString(2, gd.strings.remove(0));
                cs.setInt(3, gd.ints.remove(0)); // 1 for true, 0 for false
                cs.setInt(4, gd.ints.remove(0));
                cs.setInt(5, gd.ints.remove(0));
                break;

            case "sceneequipment":
            case "sceneprops":
                // SceneID, AssetID
                cs.setInt(1, gd.ints.remove(0));
                cs.setInt(2, gd.ints.remove(0));
                break;

            case "acts":
                // ActorID, SceneID, PropID
                cs.setInt(1, gd.ints.remove(0));
                cs.setInt(2, gd.ints.remove(0));
                cs.setInt(3, gd.ints.remove(0));
                break;

            default:
                // Log error or throw exception for unknown table
        }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public static class genericData {
        public ArrayList<Integer> ints = new ArrayList<>();
        public ArrayList<String> strings = new ArrayList<>();

        public void addInts(int toAdd){
            this.ints.add(toAdd);
        }

        public void addStrings(String toAdd){
            this.strings.add(toAdd);
        }
    }
}
