package uk.ac.bris.cs.databases.cwk2;

import java.sql.Connection;
import java.util.*;
import uk.ac.bris.cs.databases.api.APIProvider;
import uk.ac.bris.cs.databases.api.AdvancedForumSummaryView;
import uk.ac.bris.cs.databases.api.AdvancedForumView;
import uk.ac.bris.cs.databases.api.ForumSummaryView;
import uk.ac.bris.cs.databases.api.ForumView;
import uk.ac.bris.cs.databases.api.AdvancedPersonView;
import uk.ac.bris.cs.databases.api.PostView;
import uk.ac.bris.cs.databases.api.Result;
import uk.ac.bris.cs.databases.api.PersonView;
import uk.ac.bris.cs.databases.api.SimpleForumSummaryView;
import uk.ac.bris.cs.databases.api.SimpleTopicView;
import uk.ac.bris.cs.databases.api.TopicView;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

    public API(Connection c) {
        this.c = c;
    }

    /* A.1 */

    @Override
    public Result<Map<String, String>> getUsers() {
        Result<Map<String, String>> result;
        
        try {
            PreparedStatement s = c.prepareStatement(
                "SELECT username, name FROM Person"
            );
            Map<String, String> map = new HashMap<String, String>();
            ResultSet r = s.executeQuery();
            while (r.next()) {
                String username = r.getString("username");
                String name = r.getString("name");
                map.put(username, name);
            }
            result = Result.success(map);
            s.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        if(result.isSuccess()) System.out.println("getUsers Function sucessfuly excecuted");
        return result;
    }

    @Override
    public Result<PersonView> getPersonView(String username) {
        Result<PersonView> result;
        try {
            PreparedStatement s = c.prepareStatement(
                "SELECT name, username, stuId FROM Person WHERE username = ? "
            );
            s.setString(1,username);
            ResultSet r = s.executeQuery();
            PersonView resultview = null;
            if(r.next())
            {   String name = r.getString("name");
                String stuId = r.getString("stuId");
                if(stuId == null) stuId = "null";
                resultview = new PersonView(name,username,stuId);
            }
            result = Result.success(resultview);
            s.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        if(result.isSuccess()) System.out.println("getPersonView Function sucessfuly excecuted");
        return result;
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        Result result;

        try {
            PreparedStatement s = c.prepareStatement(
                "INSERT INTO Person (name, username, stuId) VALUES (?, ?, ?)"
            );
            s.setString(1,name);
            s.setString(2,username);
            s.setString(3,studentId);
            s.executeUpdate();
            result = Result.success();
            s.close();
            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (SQLException f) {
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
        Result<List<SimpleForumSummaryView>> result;

        try {
            PreparedStatement s = c.prepareStatement(
                "SELECT id, title FROM Forum"
            );
            List<SimpleForumSummaryView> list = new ArrayList<SimpleForumSummaryView>();
            SimpleForumSummaryView sfsv = null;
            ResultSet r = s.executeQuery();
            while (r.next()) {
                int id = r.getInt("id");
                String title = r.getString("title");
                sfsv = new SimpleForumSummaryView(id, title);
                list.add(sfsv);
            }
            result = Result.success(list);
            s.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    @Override
    public Result createForum(String title) {
        Result result;
        if (title == null) {
            return Result.failure("Forum title can not be NULL!");
        }
        if (title.equals("")) {
            return Result.failure("Forum title can not be empty!");
        }
        for (SimpleForumSummaryView item : getSimpleForums().getValue()) {
            if (title.equals(item.getTitle())) {
                return Result.failure("Forum existed!");
            }
        }

        try {
            PreparedStatement s = c.prepareStatement(
                "INSERT INTO Forum (title) VALUES (?)"
            );
            s.setString(1, title);
            s.executeUpdate();
            result = Result.success(title);
            s.close();
            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
            } catch (SQLException f) {
                return Result.fatal(f.getMessage());
            }
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    /* A.3 */

    @Override
    public Result<List<ForumSummaryView>> getForums() {
       System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<ForumView> getForum(int id) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<PostView> getLatestPost(int topicId) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.1 */

    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
      System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
