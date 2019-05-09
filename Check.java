package uk.ac.bris.cs.databases.cwk2;

import uk.ac.bris.cs.databases.api.Result;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

class Check {

    /**
     * Check {@link API#getUserId(String) getUserId} to see their similar functionality
     * @param id
     * @return
     */
    static Result checkTopicId(int id, Connection c) {
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
     * Check {@link API#getUserId(String) getUserId} to see their similar functionality
     * @param forumId
     * @return
     */
    static Result checkForumId(int forumId, Connection c) {
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
     * Check {@link API#getUserId(String) getUserId} to see their similar functionality
     * @param title
     * @return
     */
    static Result checkForumTitle(String title, Connection c) {
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