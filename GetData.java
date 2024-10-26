import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Your implementation goes here....
            ResultSet rs = stmt.executeQuery(
                "SELECT * FROM " + userTableName);
            
            while (rs.next()) {
                JSONObject user = new JSONObject(); 
                int user_id = rs.getInt("USER_ID");
                user.put("user_id", user_id); 
                user.put("first_name", rs.getString("FIRST_NAME"));
                user.put("last_name", rs.getString("LAST_NAME"));
                user.put("YOB", rs.getInt("YEAR_OF_BIRTH"));
                user.put("MOB", rs.getInt("MONTH_OF_BIRTH")); 
                user.put("DOB", rs.getInt("DAY_OF_BIRTH"));
                user.put("gender", rs.getString("GENDER"));

                String friendsQuery = "SELECT user2_id FROM " + friendsTableName + 
                        " WHERE user1_id = " + user_id + 
                        " AND user2_id > " + user_id;
                Statement friendsStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet friends = friendsStmt.executeQuery(friendsQuery);
                JSONArray friendsArray = new JSONArray();
                while (friends.next()) {
                    friendsArray.put(friends.getInt("user2_id"));
                }
                user.put("friends", friendsArray);
                friendsStmt.close();

                String hometownQuery = "SELECT c.city_name, c.state_name, c.country_name FROM " + hometownCityTableName + " hc " +
                                        "JOIN " + cityTableName + " c ON c.city_id = hc.hometown_city_id " +
                                        "WHERE user_id = " + user_id;
                Statement hometownStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet hometownRs = hometownStmt.executeQuery(hometownQuery);
                JSONObject hometownCity = new JSONObject();
                if (hometownRs.next()) {
                    hometownCity.put("country", hometownRs.getString("country_name"));
                    hometownCity.put("state", hometownRs.getString("state_name"));
                    hometownCity.put("city", hometownRs.getString("city_name"));

                }
                user.put("hometown", hometownCity);
                hometownStmt.close();

                String currentCityQuery = "SELECT c.city_name, c.state_name, c.country_name FROM " + currentCityTableName + " cc " +
                                        "JOIN " + cityTableName + " c ON c.city_id = cc.current_city_id " +
                                        "WHERE user_id = " + user_id;
                Statement currentCityStmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet currentRs = currentCityStmt.executeQuery(currentCityQuery);
                JSONObject currentCity = new JSONObject();
                if (currentRs.next()) {
                    currentCity.put("country", currentRs.getString("country_name"));
                    currentCity.put("state", currentRs.getString("state_name"));
                    currentCity.put("city", currentRs.getString("city_name"));

                }
                user.put("current", currentCity);
                currentCityStmt.close();
                
                users_info.put(user);
            }
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
