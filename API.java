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
import uk.ac.bris.cs.databases.api.SimpleTopicSummaryView;
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
    public Result<PersonView> getPersonView(String username)
    {   Result<PersonView> result;
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
                if(stuId==null) stuId = "null";
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
        System.out.println("fdf");
        Result result = null;

        try {
            PreparedStatement s = c.prepareStatement(
            "INSERT INTO Person (name, username, stuId) VALUES(?, ?, ?)");
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
       System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createForum(String title) {
       System.out.println("fdf");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* A.3 */

    @Override
    public Result<List<ForumSummaryView>> getForums() {
        Result<List<ForumSummaryView>> result;
        try{
            PreparedStatement s = c.prepareStatement(
                "SELECT title, id FROM Forum ORDER BY title ASC"
            );
            List<ForumSummaryView> resultView = new ArrayList<>();
            ResultSet r = s.executeQuery();

            while(r.next()){
                int forumId = r.getInt("id");
                String forumTitle = r.getString("title");
                
                // construct the SimpleTopicSummaryView which is the last element of ForumSummaryView
                PreparedStatement s2 = c.prepareStatement("SELECT topic.topicId, topic.forumId, topic.title " +
                    "FROM forum JOIN topic ON forum.id = topic.forumId " +
                            "JOIN post ON topic.topicId = post.topicId " +
                    "WHERE forum.id = ? " +
                    "ORDER BY post.postedAt DESC " +
                    "LIMIT 1");
                s2.setInt(1, forumId);
                ResultSet r2 = s2.executeQuery();
                if(r2.next()){
                    int topicId = r2.getInt("topicId");
                    String topicTitle = r2.getString("title");
                    SimpleTopicSummaryView lastTopic = new SimpleTopicSummaryView(topicId, forumId, topicTitle);
                    s2.close();
                    ForumSummaryView forumSummaryView = new ForumSummaryView(forumId, forumTitle, lastTopic);
                    resultView.add(forumSummaryView);
                }
            }
            result = Result.success(resultView);
            s.close();
        } catch(SQLException e){
            result = Result.fatal(e.getMessage());
        }
        if(result.isSuccess()) System.out.println("getForums Function successfully executed!");
        return result;
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
        Result<PostView> result;
        try{
            PreparedStatement s1 = c.prepareStatement(
                "SELECT Topic.forumId AS forum, Person.name AS authorName," +
                   "Person.username AS authorUserName, text, postedAt, postLike.likes AS likes FROM Topic" +
            "INNER JOIN Post ON Topic.topicId = Post.topicId" +
            "INNER JOIN Person ON Post.authorId = Person.id" +
            "LEFT JOIN (" +
                "SELECT Post.postId AS postId, COUNT(*) AS likes FROM Post" +
                "INNER JOIN PersonLikePost ON PersonLikePost.postId = Post.postId" +
            ")AS postLike ON postLike.postId = Post.postId" +
            "WHERE Topic.topicId = ?" +
            "ORDER BY postedAt DESC" +
            "LIMIT 1");
            ResultSet r = s1.executeQuery();

            PreparedStatement s2 = c.prepareStatement("SELECT COUNT(*) AS PostNumber FROM" +
            "(  SELECT Post.postedAt as postedAt FROM" +
               "Topic JOIN Post ON Topic.topicId = Post.topicId" +
               "WHERE Topic.topicId = ?  ORDER BY Post.postedAt ASC) AS a" +
            "WHERE postedAt <=" +
            "(  SELECT postedAt FROM Post WHERE Post.postId = ? )");
            ResultSet r2 = s2.executeQuery();

            int forumId = r.getInt("forum");
            int postNumber = r2.getInt("PostNumber");
            String authorName = r.getString("authorName");
            String authorUserName = r.getString("authorUserName");
            String text = r.getString("text");
            String postedAt = r.getString("postedAt");
            int likes = r.getInt("likes");

            PostView resultView = new PostView(forumId, topicId, postNumber,
                authorName, authorUserName, text, postedAt, likes);
            result = Result.success(resultView);
            s1.close();
            s2.close();
        } catch(SQLException e){
            result = Result.failure("failure");
        }
        if(result.isSuccess()) System.out.println("getLatesPost Function successfully executed!");
        return result;
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
