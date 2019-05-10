package uk.ac.bris.cs.databases.cwk2;

import uk.ac.bris.cs.databases.api.Result;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

public class Check {
    /**
    * This method encapsulated several checks related to username(String), including
    * NULL check, Empty check, and existance check
    * @param username the username of the person trying to do operations(add,lookup,delete)
    * @param c the connectio of the database
    * @return Return {@code Result.success} if all check pass
    * Return {@code Result.failure} with correspond information
    * Return {@code Result.fatal} if there is a sql exception
    */
    public static Result usernameCheck(String username, Connection c) {
       Result basicCheck = Check.usernameBaiscCheck(username);
       if(!basicCheck.isSuccess())
            return basicCheck;
       Result existanceCheck = Check.usernameCheckExistance(username,c);
       if(!existanceCheck.isSuccess())
            return existanceCheck;
       return Result.success();
     }

     /**
     * This method checks if the username is empty or null
     * @param username the username of the person)
     * @return Return {@code Result.success} if all check pass
     * Return {@code Result.failure} if is null or empty
     */
    public static Result usernameBaiscCheck(String username){
       if (username == null)
            return Result.failure("Username can not be null.");
       if (username.equals(""))
            return Result.failure("Username can not be blank.");
       return Result.success();
    }

    /**
    * This method checks if the username exists
    * @param username the username of the person)
    * @param c the connectio of the database
    * @return Return {@code Result.success} if username exists
    * Return {@code Result.failure} if there is no such username
    * Return {@code Result.fatal} if is sql exception
    */
    public static Result usernameCheckExistance(String username, Connection c){
       String q = "SELECT * FROM Person WHERE Person.username = ?";
       try (PreparedStatement s = c.prepareStatement(q)) {
            s.setString(1,username);
            ResultSet r = s.executeQuery();
            if (r.next())
               return Result.success();
            else
               return Result.failure("There is no such username");
       } catch (SQLException e) {
            return Result.fatal(e.getMessage());
       }
    }

    /**
     * Check {@link API#usernameCheck(String) usernameCheck} to see their similar functionality
     * @param id
     * @param c the connectio of the database
     * @return
     */
    public static Result checkTopicId(int id, Connection c) {
        String q = "SELECT * FROM Topic WHERE topicId = ?";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,id);
            ResultSet r = s.executeQuery();
            if (r.next())
                return Result.success();
            else
                return Result.failure("There is no such topic");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    /**
     * Check {@link API#usernameCheck(String) usernameCheck} to see their similar functionality
     * @param forumId
     * @param c the connectio of the database
     * @return
     */
     public static Result checkForumId(int forumId, Connection c) {
        String q = "SELECT * FROM Forum WHERE id = ?";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,forumId);
            ResultSet r = s.executeQuery();
            if (r.next())
                return Result.success();
            else
                return Result.failure("There is no such forum");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    /**
     * Check {@link API#usernameCheck(String) usernameCheck} to see their similar functionality
     * @param c the connectio of the database
     * @param title
     * @return
     */
    public static Result checkForumTitle(String title, Connection c) {
        String q = "SELECT * FROM Forum WHERE title = ?";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setString(1, title);
            ResultSet r = s.executeQuery();
            if (r.next())
                return Result.failure("Forum already exists");
            else
                return Result.success();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }
}
