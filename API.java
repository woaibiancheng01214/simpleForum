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
import uk.ac.bris.cs.databases.api.SimplePostView;
import uk.ac.bris.cs.databases.api.TopicView;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
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
        if (result.isSuccess()) System.out.println("getUsers Function sucessfuly excecuted");
        return result;
    }

    @Override
    public Result<PersonView> getPersonView(String username) {
        Result<PersonView> result = null;
        try {
            PreparedStatement s = c.prepareStatement(
                "SELECT name, username, stuId FROM Person WHERE username = ? "
            );
            s.setString(1,username);
            ResultSet r = s.executeQuery();
            PersonView resultview = null;
            if (r.next())
            {   String name = r.getString("name");
                String stuId = r.getString("stuId");
                if (stuId == null) stuId = "null";
                resultview = new PersonView(name,username,stuId);
            }
            result = Result.success(resultview);
            s.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        if (result.isSuccess()) System.out.println("getPersonView Function sucessfuly excecuted");
        return result;
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        Result result = null;

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
        Result<List<SimpleForumSummaryView>> result = null;

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
        Result result = null;
        if (title == null) {
            return Result.failure("Forum title can not be NULL!");
        }
        if (title.equals("")) {
            return Result.failure("Forum title can not be empty!");
        }

        // the checking of existing forums here might be simplified
        // by a query? or?

        List<SimpleForumSummaryView> listOfForums = getSimpleForums().getValue();
        for (SimpleForumSummaryView item : listOfForums) {
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
        Result<List<ForumSummaryView>> result = null;
        try{
            // find (forumId,lastTopicView) pairs
            // if there're several topics have latest posts at the same time, an arbitrary one is chosed(no rules)
            Map<Integer,SimpleTopicSummaryView> forumToTopicMapping = new HashMap<>();
            PreparedStatement s2 = c.prepareStatement(
            "SELECT topicId, a.forumId as forumId, topicTitle FROM " +
            " ( SELECT topic.topicId, topic.forumId, topic.title as topicTitle, post.postedAt " +
            " FROM forum JOIN topic ON forum.id = topic.forumId " +
            "   JOIN post ON topic.topicId = post.topicId ) AS a " +
            "JOIN " +
            "( SELECT topic.forumId, MAX(postedAt) as latest " +
               " FROM forum JOIN topic ON forum.id = topic.forumId "+
                     " JOIN post ON topic.topicId = post.topicId GROUP BY forumId ) AS b "+
            "ON a.forumId = b.forumId AND a.postedAt = b.latest GROUP BY a.forumId" );
            ResultSet r2 = s2.executeQuery();
            while(r2.next())
            {   int forumId = r2.getInt("forumId");
                int topicId = r2.getInt("topicId");
                String topicTitle = r2.getString("topicTitle");
                SimpleTopicSummaryView lastTopic = new SimpleTopicSummaryView(topicId, forumId, topicTitle);
                forumToTopicMapping.put(forumId,lastTopic);
            }
            s2.close();

            // add lasttopic to forumView
            PreparedStatement s = c.prepareStatement(
                "SELECT title, id FROM Forum ORDER BY title ASC"
            );
            ResultSet r = s.executeQuery();
            List<ForumSummaryView> resultView = new ArrayList<>();
            while(r.next())
            {   int forumId = r.getInt("id");
                String forumTitle = r.getString("title");
                SimpleTopicSummaryView lastTopic = forumToTopicMapping.get(forumId);
                // here lastTopic is allowed to be null
                ForumSummaryView forumSummaryView = new ForumSummaryView(forumId, forumTitle, lastTopic);
                resultView.add(forumSummaryView);
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
        Result<ForumView> result = null;
        Boolean forumexists = false;
        for (SimpleForumSummaryView item : getSimpleForums().getValue()) {
            if (id == item.getId()) {
                forumexists = true;
            }
        }
        if (forumexists == false) {
            return Result.failure("Forum doesn't exist!");
        }

        try {
            PreparedStatement stsvs = c.prepareStatement(
                "SELECT topicId, forumId, title FROM Topic " +
                "WHERE forumId = ? ORDER BY title ASC"
            );
            stsvs.setInt(1, id);
            ResultSet stsvr = stsvs.executeQuery();
            List<SimpleTopicSummaryView> topiclist = new ArrayList<SimpleTopicSummaryView>();
            SimpleTopicSummaryView stsv;
            while (stsvr.next()) {
                int topicid = stsvr.getInt("topicId");
                String topictitle = stsvr.getString("title");
                stsv = new SimpleTopicSummaryView(topicid, id, topictitle);
                topiclist.add(stsv);
            }
            ForumView resultview = null;
            PreparedStatement fvs = c.prepareStatement(
                "SELECT * FROM Forum WHERE id = ?"
            );
            fvs.setInt(1, id);
            ResultSet fvr = fvs.executeQuery();
            while (fvr.next()) {
                String forumtitle = fvr.getString("title");
                resultview = new ForumView(id, forumtitle, topiclist);
            }
            result = Result.success(resultview);
            stsvs.close();
            fvs.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
        Result<SimpleTopicView> result = null;

        try {
            PreparedStatement spvs = c.prepareStatement(
                "SELECT topicId, Person.name AS authorUserName, " +
                "text, postedAt FROM Post " +
                "INNER JOIN Person ON Post.authorId = Person.Id " +
                "WHERE topicId = ? ORDER BY Post.postedAt ASC"
            );
            spvs.setInt(1, topicId);
            ResultSet spvr = spvs.executeQuery();
            SimpleTopicView resultview = null;
            List<SimplePostView> postlist = new ArrayList<SimplePostView>();
            SimplePostView spv = null;
            int postNumber = 0;
            while (spvr.next()) {
                String authorUserName = spvr.getString("authorUserName");
                String text = spvr.getString("text");
                String postedAt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(spvr.getTimestamp("postedAt"));
                postNumber++;
                spv = new SimplePostView(postNumber, authorUserName, text, postedAt);
                postlist.add(spv);
            }
            PreparedStatement stvs = c.prepareStatement(
                "SELECT topicId, title FROM Topic WHERE topicId = ?"
            );
            stvs.setInt(1, topicId);
            ResultSet stvr = stvs.executeQuery();
            if (stvr.next() == false) {
                return Result.failure("Topic doesn't exist!");
            } else {
                do {
                    String title = stvr.getString("title");
                    resultview = new SimpleTopicView(topicId, title, postlist);
                } while (stvr.next());
            }
            result = Result.success(resultview);
            spvs.close();
            stvs.close();
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    @Override
    public Result<PostView> getLatestPost(int topicId) {
        Result<PostView> result = null;
        try{
            PreparedStatement s = c.prepareStatement(
                "SELECT Topic.forumId AS forum, Person.name AS authorName, " +
                   "Person.username AS authorUserName, text, postedAt, COUNT(*) AS postNumber, " +
                   "postLike.likes AS likes FROM Topic " +
            "INNER JOIN Post ON Topic.topicId = Post.topicId " +
            "INNER JOIN Person ON Post.authorId = Person.id " +
            "LEFT JOIN ( " +
                "SELECT Post.postId AS postId, COUNT(*) AS likes FROM Post " +
                "INNER JOIN PersonLikePost ON PersonLikePost.postId = Post.postId " +
            ")AS postLike ON postLike.postId = Post.postId " +
            "WHERE Topic.topicId = ? " +
            "ORDER BY postedAt DESC " +
            "LIMIT 1");
            ResultSet r = s.executeQuery();

            if(r.next()){
                int forumId = r.getInt("forum");
                int postNumber = r.getInt("PostNumber");
                String authorName = r.getString("authorName");
                String authorUserName = r.getString("authorUserName");
                String text = r.getString("text");
                String postedAt = r.getString("postedAt");
                int likes = r.getInt("likes");

                PostView resultView = new PostView(forumId, topicId, postNumber,
                    authorName, authorUserName, text, postedAt, likes);
                result = Result.success(resultView);
            }
            s.close();
        } catch(SQLException e){
            result = Result.failure("failure");
        }
        if(result.isSuccess()) System.out.println("getLatesPost Function successfully executed!");
        return result;
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
        Result result = null;
        if (username == null || text == null) {
            return Result.failure("username or text can not be NULL!");
        }
        if (username.equals("") || text.equals("")) {
            return Result.failure("Author's username or Post's text can not be empty!");
        }
      // the situation topicId fails the foreign key constrain should return
      // failure or fatal?  currently it will fall into SQLException and fatal
        try {
            PreparedStatement s0 = c.prepareStatement(
               "SELECT id FROM Person WHERE Person.username = ?"
            );
            s0.setString(1,username);
            ResultSet r = s0.executeQuery();
            int authorId;
            if(r.next()){ authorId = r.getInt("id");}
            else return Result.failure("Author's username doesn't exit");

            PreparedStatement s = c.prepareStatement(
                "INSERT INTO Post (topicId,text,authorId) VALUES ( ? , ? ,? )"
            );
            s.setInt(1,topicId);
            s.setString(2, text);
            s.setInt(3,authorId);

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
        if(result.isSuccess()) System.out.println("createPost Function successfully executed!");
        return result;
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
