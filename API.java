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

import uk.ac.bris.cs.databases.api.TopicSummaryView;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
/** compile issues
 * there is a unsafe type uncheck conversion problem, (-XLINT: uncheckED)
 * i don't think it is solvable because of the nature of the result class ////by wangchao  @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

    public API(Connection c) {
        this.c = c;
    }

    /* A.1 */

    @Override
    public Result<Map<String, String>> getUsers() {
        Map<String, String> map = new HashMap<String, String>();
        String q = "SELECT username, name FROM Person";
        try (PreparedStatement s = c.prepareStatement(q)) {
            ResultSet r = s.executeQuery();
            while (r.next()) {
                String username = r.getString("username");
                String name = r.getString("name");
                map.put(username, name);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(map);
    }


    //failure case not existing
    @Override
    public Result<PersonView> getPersonView(String username) {
        Result usernameCheck = usernameCheck(username);
        if (!usernameCheck.isSuccess()) {
            return usernameCheck;
        }

        PersonView resultview = null;
        String q = "SELECT name, username, stuId FROM Person WHERE username = ? ";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setString(1,username);
            ResultSet r = s.executeQuery();
            if (r.next()) {
                String name = r.getString("name");
                String stuId = r.getString("stuId");
                if (stuId == null)
                    stuId = "";

                resultview = new PersonView(name,username,stuId);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(resultview);
    }

    /**
     * This method encapsulated several checks related to username(String), including
     * NULL check, Empty check, and existance check (by {@link #getUserId(String) getUserId})
     * @param username the username of the person trying to do operations(add,lookup,delete)
     * @return Return {@code Result.success} if all check pass
     * Return {@code Result.failure} with correspond information
     */
    private Result usernameCheck(String username) {
        if (username == null)
            return Result.failure("Username can not be null.");
        if (username.equals(""))
            return Result.failure("Username can not be blank.");
        Result usernameCheck = getUserId(username);
        if (!usernameCheck.isSuccess())
            return usernameCheck;
        return Result.success();
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        if (username == null)
            return Result.failure("Username can not be null.");
        if (username.equals(""))
            return Result.failure("Username can not be blank.");
        // found existing username, return failre
        // sql fatal error, return fatal
        Result usernameCheck = getUserId(username);
        if (usernameCheck.isSuccess())
            return Result.failure("Username already exists");
        else if (usernameCheck.isFatal())
            return usernameCheck;

        String q = "INSERT INTO Person (name, username, stuId) VALUES (?, ?, ?)";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setString(1,name);
            s.setString(2,username);
            s.setString(3,studentId);
            s.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        return Result.success();
    }

    /**
     * This method is used for trying to rollback to last commit when
     * a SQLException is caught during updating database.
     * @param e the original SQLException caught by previous method
     * @return return original SQLException {@code e.getMessage()} if 
     * successfully rollback, and return new SQLException {@code f.getMessage()}
     * if failed to rollback.
     */
    private Result tryRollback(SQLException e) {
        try {
            c.rollback();
        } catch (SQLException f) {
            return Result.fatal(f.getMessage());
        }
        return Result.fatal(e.getMessage());
    }

    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
        String q = "SELECT id, title FROM Forum ORDER BY title ASC";
        try (PreparedStatement s = c.prepareStatement(q)) {
            List<SimpleForumSummaryView> list = new ArrayList<SimpleForumSummaryView>();
            SimpleForumSummaryView simpleForumSummaryView = null;
            ResultSet r = s.executeQuery();
            while (r.next()) {
                int id = r.getInt("id");
                String title = r.getString("title");
                simpleForumSummaryView = new SimpleForumSummaryView(id, title);
                list.add(simpleForumSummaryView);
            }
            return Result.success(list);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result createForum(String title) {
        if (title == null)
            return Result.failure("Forum title can not be NULL!");
        if (title.equals(""))
            return Result.failure("Forum title can not be empty!");

        Result forumTitleCheck = Check.checkForumTitle(title,c);
        if (!forumTitleCheck.isSuccess())
            return forumTitleCheck;

        String q = "INSERT INTO Forum (title) VALUES (?)";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setString(1, title);
            s.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }

        return Result.success(title);
    }

    /* A.3 */
    @Override
    public Result<List<ForumSummaryView>> getForums() {
        List<ForumSummaryView> resultView = new ArrayList<>();
        // find (forumId,lastTopicView) pairs
        // if there're several topics have latest posts at the same time, an arbitrary one is chosed(no rules)
        Map<Integer,SimpleTopicSummaryView> forumToTopicMapping = new HashMap<>();
        String q1 = "SELECT topicId, a.forumId as forumId, topicTitle " +
                    "FROM " +
                    " ( SELECT Topic.topicId, Topic.forumId, Topic.title as topicTitle, Post.postId " +
                    " FROM Forum JOIN Topic ON Forum.id = Topic.forumId " +
                    "   JOIN Post ON Topic.topicId = Post.topicId ) AS a " +
                    "JOIN " +
                    "( SELECT Topic.forumId, MAX(postId) as latest " +
                    " FROM Forum JOIN Topic ON Forum.id = Topic.forumId "+
                    " JOIN Post ON Topic.topicId = Post.topicId GROUP BY forumId ) AS b "+
                    "ON a.forumId = b.forumId AND a.postId = b.latest GROUP BY a.forumId";
        try (PreparedStatement s1 = c.prepareStatement(q1)) {
            ResultSet r1 = s1.executeQuery();
            while (r1.next()) {
                int forumId = r1.getInt("forumId");
                int topicId = r1.getInt("topicId");
                String topicTitle = r1.getString("topicTitle");
                SimpleTopicSummaryView lastTopic = new SimpleTopicSummaryView(topicId, forumId, topicTitle);
                forumToTopicMapping.put(forumId,lastTopic);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        String q2 = "SELECT title, id FROM Forum ORDER BY title ASC";
        try (PreparedStatement s2 = c.prepareStatement(q2)) {
            // add lastTopic to forumView
            ResultSet r2 = s2.executeQuery();
            while (r2.next()) {
                int forumId = r2.getInt("id");
                String forumTitle = r2.getString("title");
                SimpleTopicSummaryView lastTopic = forumToTopicMapping.get(forumId);
                // here lastTopic is unchecked so it is allowed to be null
                ForumSummaryView forumSummaryView = new ForumSummaryView(forumId, forumTitle, lastTopic);
                resultView.add(forumSummaryView);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return Result.success(resultView);
    }


    @Override
    public Result<ForumView> getForum(int id) {
        Result forumIdCheck = Check.checkForumId(id,c);
        if (!forumIdCheck.isSuccess())
            return forumIdCheck;

        List<SimpleTopicSummaryView> topiclist;
        String q1 = "SELECT topicId, forumId, Topic.title, Forum.title AS forumTitle FROM Topic " +
                    "INNER JOIN Forum ON Topic.forumId = Forum.id " +
                    "WHERE forumId = ? ORDER BY Topic.title ASC";
        try (PreparedStatement s1 = c.prepareStatement(q1)) {
            s1.setInt(1, id);
            ResultSet r1 = s1.executeQuery();
            topiclist = new ArrayList<SimpleTopicSummaryView>();
            String forumTitle = "";
            if(r1.next()) {
                forumTitle = r1.getString("forumTitle");
                do {
                    int topicid = r1.getInt("topicId");
                    String topictitle = r1.getString("title");
                    topiclist.add(new SimpleTopicSummaryView(topicid, id, topictitle));
                } while (r1.next());
            }
            ForumView resultview = new ForumView(id, forumTitle, topiclist);
            return Result.success(resultview);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
        Result topicIdCheck = Check.checkTopicId(topicId, c);
        if (!topicIdCheck.isSuccess())
            return topicIdCheck;

        SimpleTopicView resultview = null;
        List<SimplePostView> postlist;
        String q1 = "SELECT Topic.topicId, Person.name AS authorUserName, " +
                    "   text, postedAt, Topic.title AS topicTitle FROM Post " +
                    "INNER JOIN Person ON Post.authorId = Person.Id " +
                    "INNER JOIN Topic ON Post.topicId = Topic.topicId " +
                    "WHERE Topic.topicId = ? ORDER BY Post.postedAt ASC";

        try (PreparedStatement s1 = c.prepareStatement(q1)) {
            s1.setInt(1, topicId);
            ResultSet r1 = s1.executeQuery();
            postlist = new ArrayList<SimplePostView>();
            int postNumber = 0;
            String topicTitle = "";
            if(r1.next()){
                topicTitle = r1.getString("topicTitle");
                do {
                    String authorUserName = r1.getString("authorUserName");
                    String text = r1.getString("text");
                    String postedAt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(r1.getTimestamp("postedAt"));
                    postNumber++;
                    postlist.add(new SimplePostView(postNumber, authorUserName, text, postedAt));
                } while (r1.next());
            }
            resultview = new SimpleTopicView(topicId, topicTitle, postlist);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return Result.success(resultview);
    }

    @Override
    public Result<PostView> getLatestPost(int topicId) {
        PostView resultView = null;
        String q = "SELECT Topic.forumId AS forum, Person.name AS authorName, " +
                    "   Person.username AS authorUserName, text, postedAt, COUNT(*) AS postNumber, " +
                    "   postLike.likes AS likes" +
                    "FROM Topic " +
                    "INNER JOIN Post ON Topic.topicId = Post.topicId " +
                    "INNER JOIN Person ON Post.authorId = Person.id " +
                    "LEFT JOIN" +
                    "   (SELECT Post.postId AS postId, COUNT(*) AS likes FROM Post " +
                    "   INNER JOIN PersonLikePost ON PersonLikePost.postId = Post.postId " +
                    "   )AS postLike ON postLike.postId = Post.postId " +
                    "WHERE Topic.topicId = ? " +
                    "ORDER BY postedAt DESC " +
                    "LIMIT 1";

        try (PreparedStatement s = c.prepareStatement(q)) {
            ResultSet r = s.executeQuery();
            if (r.next()) {
                int forumId = r.getInt("forum");
                int postNumber = r.getInt("PostNumber");
                String authorName = r.getString("authorName");
                String authorUserName = r.getString("authorUserName");
                String text = r.getString("text");
                String postedAt = r.getString("postedAt");
                int likes = r.getInt("likes");

                resultView = new PostView(forumId, topicId, postNumber,
                    authorName, authorUserName, text, postedAt, likes);
            }
        } catch(SQLException e) {
            return Result.failure("failure");
        }
        return Result.success(resultView);
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
        if (username == null || text == null)
            return Result.failure("username or text can not be NULL!");

        if (username.equals("") || text.equals(""))
            return Result.failure("Author's username or Post's text can not be empty!");

        // topicId checking
        Result topicIdCheck = Check.checkTopicId(topicId,c);
        if (!topicIdCheck.isSuccess())
            return topicIdCheck;

        // fetch userId and check
        Result<Integer> userIdResult = getUserId(username);
        if (!userIdResult.isSuccess())
           return userIdResult;

        int authorId = userIdResult.getValue().intValue();
        return createPostFromUserId(topicId,authorId,text);
    }

    /**
     * This method is used for spliting {@code createPost} method to two 
     * seperate tasks (parameter check and query execution)
     * @param topicId  the id of the topic that this new post belongs to
     * @param authorId the id of the person who created this post
     * @param text     the content of this post
     * @return return {@code Result.success} if a post is successfully created.
     * Check {@link #tryRollback(SQLException e) tryRollback} for other types returns 
     */
    private Result createPostFromUserId(int topicId, int authorId, String text) {
      // everything is already checked in previous function
      String q = "INSERT INTO Post (topicId,text,authorId) VALUES ( ? , ? ,? )";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,topicId);
            s.setString(2, text);
            s.setInt(3,authorId);

            s.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        return Result.success();
    }

    /**
     * This method is used for check whether the person with a particular username
     * exists in the database every time a method receive a username as the parameter.
     * Besides, {@link Check#checkTopicId(int) checkTopicId}, {@link Check#checkForumId(int) checkForumId} 
     * and {@link Check#checkForumTitle(String) checkForumTitle} #chec
     * appear to have similar functionality with this method.
     * @param username the username of the person who are doing operations(add,delete,update)
     * @return Return {@code Result.success} if the person with that username is found
     * in database. 
     * Return {@code Result.failure} if not found. 
     * Return {@code Result.fatal} if SQLException is caught when execute lookup query
     */
    private Result<Integer> getUserId(String username) {
        Integer userId = null;
        String q = "SELECT id FROM Person WHERE Person.username = ?";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setString(1,username);
            ResultSet r = s.executeQuery();
            if (r.next())
               userId = Integer.valueOf(r.getInt("id"));
            else
                return Result.failure("There is no such username");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return Result.success(userId);
    }


    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        Result usernameCheck = usernameCheck(username);
        if (!usernameCheck.isSuccess()) {
            return usernameCheck;
        }
        if (text == null || title == null)
            return Result.failure("text or topic title can not be NULL!");
        if (text.equals("") || title.equals(""))
            return Result.failure("initial Post's text or topic's title can not be empty!");

        // forumId checking
        Result forumIdCheck = Check.checkForumId(forumId,c);
        if (!forumIdCheck.isSuccess())
            return forumIdCheck;

        //fetch userId and checking
        Result<Integer> userIdResult = getUserId(username);
        if (!userIdResult.isSuccess())
            return userIdResult;

        String q1 = "INSERT INTO Topic (forumId, title, creatorId) VALUES (? ,? , ?) ";
        try (PreparedStatement s1 = c.prepareStatement(q1)) {
            int creatorId = userIdResult.getValue().intValue();
            s1.setInt(1,forumId);
            s1.setString(2,title);
            s1.setInt(3,creatorId);
            s1.executeUpdate();
            c.commit();

            int topicId;
            String q2 = "SELECT LAST_INSERT_ID()";
            try (PreparedStatement s2 = c.prepareStatement(q2)) {
                ResultSet r = s2.executeQuery();
                if (r.next())
                    topicId = r.getInt("LAST_INSERT_ID()");
                else
                    return Result.fatal("function --- LAST_INSERT_ID() in mariadb failed");
            } catch(SQLException e) {
                return Result.fatal(e.getMessage());
            }

            // using function createPostFromUserId() to save one getUserId query
            Result firstPostResult = createPostFromUserId(topicId,creatorId,text);
            if (!firstPostResult.isSuccess())
                return firstPostResult;

        } catch (SQLException e) {
            return tryRollback(e);
        }
        return Result.success();
    }

   /* as mentioned in the offical documentation
       this function is never used on web interface, therefore not tested yet*/
    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        String q = "SELECT COUNT(*) FROM Post JOIN Topic ON Post.topicId = Topic.topicId WHERE Topic.topicId = ? ";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,topicId);
            ResultSet r = s.executeQuery();
            if (r.next())
                return Result.success(Integer.valueOf(r.getInt("id")));
            else
                return Result.failure("There is no such topic with this topicId");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    /* B.1 */
    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
        Result usernameCheck = usernameCheck(username);
        if (!usernameCheck.isSuccess()) {
            return usernameCheck;
        }

        // check topicId
        Result topicIdCheck = Check.checkTopicId(topicId,c);
        if (!topicIdCheck.isSuccess()) return topicIdCheck;

        // get user's id and check
        Result<Integer> userIdResult = getUserId(username);
        if (!userIdResult.isSuccess()) return userIdResult;

        Result result = null;
        int userId = userIdResult.getValue().intValue();
        String q = "SELECT * FROM PersonLikeTopic WHERE topicId = ? AND id = ? ";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,topicId);
            s.setInt(2,userId);
            ResultSet r = s.executeQuery();

            // situation judge
            // if already like/unlike still return success as instructed
            if (r.next()) {
                if (!like)
                    result = updateLikeTopic(userId,topicId,false);
                else
                    result = Result.success();
            }
            else {
                if (!like)
                    result = Result.success();
                else
                    result = updateLikeTopic(userId,topicId,true);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    /**
     * The like/unlike records of topic are updated through this method, and it
     * is similar to another updating method {@link #updateLikePost(int, int, boolean) updateLikePost}
     * @param userId {@code int} The id of the person who clicks like/unlike
     * @param topicId {@code int} The id of the topic that is liked/unliked through click
     * @param likeOrNot {@code boolean} True means like and false denotes unlike
     * @return Return {@code Result.success} if update is successfully executed.
     * Return failure or fatal according to what happens in {@link #tryRollback(SQLException) tryRollback}
     */
    private Result updateLikeTopic(int userId, int topicId, boolean likeOrNot) {
        String q = null;
        if (likeOrNot==true)
            q = "INSERT INTO PersonLikeTopic (id,topicId) VALUES ( ? , ? )";
        else
            q = "DELETE FROM PersonLikeTopic WHERE id = ? AND topicId = ?";

        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,userId);
            s.setInt(2,topicId);
            s.executeUpdate();
            c.commit();
            return Result.success();
        } catch (SQLException e) {
            return tryRollback(e);
        }
    }

    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
        Result usernameCheck = usernameCheck(username);
        if (!usernameCheck.isSuccess()) {
            return usernameCheck;
        }
        if (post<1)
            return Result.failure("postNumber should be bigger than or equal to one");

        // check topicId
        Result topicIdCheck = Check.checkTopicId(topicId,c);
        if (!topicIdCheck.isSuccess())
            return topicIdCheck;

        // get user's id and check
        Result<Integer> userIdResult = getUserId(username);
        if (!userIdResult.isSuccess())
            return userIdResult;

        Result result = null;
        int userId = userIdResult.getValue().intValue();
        int postId;

        //get postId
        String q1 = " SELECT postId FROM Topic JOIN Post ON Topic.topicId = Post.topicId " +
                    " WHERE Topic.topicId = ? ORDER BY Post.postedAt ASC LIMIT ?,1 ";
        try (PreparedStatement s1 = c.prepareStatement(q1)) {
            s1.setInt(1,topicId);
            s1.setInt(2,post-1);
            ResultSet r = s1.executeQuery();
            if (r.next())
                postId = r.getInt("postId");
            else
                return Result.failure("this post doesn't exit");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        String q2 = "SELECT * FROM PersonLikePost WHERE id = ? AND postId = ?";
        // to like or unlike a post
        try (PreparedStatement s2 = c.prepareStatement(q2)) {
            s2.setInt(1,userId);
            s2.setInt(2,postId);
            ResultSet r = s2.executeQuery();

            // situation judge
            // if already like/unlike still return success as instructed
            if (r.next()) {
                if (!like)
                    result = updateLikePost(userId,postId,false);
                else
                    result = Result.success();
            }
            else {
                if (!like)
                    result = Result.success();
                else
                    result = updateLikePost(userId,postId,true);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    /**
     * Similar to another updating method {@link #updateLikeTopic(int, int, boolean) updateLikeTopic}
     * @param userId
     * @param postId
     * @param likeOrNot
     * @return
     */
    private Result updateLikePost(int userId, int postId, boolean likeOrNot) {
        String q = null;
        if (likeOrNot == true)
            q = "INSERT INTO PersonLikePost (id,postId) VALUES ( ? , ? )";
        else
            q = "DELETE FROM PersonLikePost WHERE id = ? AND postId = ?";

        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,userId);
            s.setInt(2,postId);
            s.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        return Result.success();
    }

    // **********this function is not in the web user iterface therefore not tested
    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        // check whether topicId exists
        Result topicIdCheck = Check.checkTopicId(topicId,c);
        if (!topicIdCheck.isSuccess())
            return topicIdCheck;

        String q =  " SELECT * FROM PersonLikeTopic " +
                    " JOIN Person ON PersonLikeTopic.id = Person.id " +
                    " WHERE topicId = ? ORDER BY Person.name ASC";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,topicId);
            ResultSet r = s.executeQuery();

            // return success even if it is an empty list
            List<PersonView> resultlist = new ArrayList<>();
            while (r.next()) {
                String name = r.getString("name");
                String stuId = r.getString("stuId");
                if (stuId == null)
                    stuId = "";

                String username = r.getString("username");
                PersonView resultview = new PersonView(name,username,stuId);
                resultlist.add(resultview);
            }
            return Result.success(resultlist);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        //topicId checking existance
        Result topicIdCheck = Check.checkTopicId(topicId,c);
        if (!topicIdCheck.isSuccess())
            return topicIdCheck;

        String q =  "SELECT Topic.topicId AS topicId, Topic.title AS topicTitle, " +
                    "   Forum.id AS forumId, Forum.title AS forumName, Post.text AS postText, " +
                    "   Person.name AS authorName, Person.username AS authorUserName, " +
                    "   COUNT(PersonLikePost.id) AS likes, postedAt " +
                    "FROM Topic " +
                    "JOIN Forum ON Topic.forumId = Forum.id " +
                    "JOIN Post ON Topic.topicId = Post.topicId "+
                    "JOIN Person ON Person.id = Post.authorId "+
                    "LEFT JOIN PersonLikePost ON Post.postId = PersonLikePost.postId "+
                    "WHERE Topic.topicId = ? GROUP BY Post.postId ORDER BY postedAt ASC";
        try (PreparedStatement s = c.prepareStatement(q)) {
            s.setInt(1,topicId);
            ResultSet r = s.executeQuery();

            List<PostView> postlist = new ArrayList<>();
            TopicView finalView = null;
            for (int i=1;r.next();i++) {
                String topicTitle = r.getString("topicTitle");
                int forumId = r.getInt("forumId");
                String forumName = r.getString("forumName");
                String postText= r.getString("postText");
                String authorName = r.getString("authorName");
                String authorUserName = r.getString("authorUserName");
                String postedAt = r.getString("postedAt");
                int likes = r.getInt("likes");
                int postNumber = i;

                PostView resultview = new PostView(forumId,topicId,postNumber,authorName,
                        authorUserName,postText,postedAt,likes);
                postlist.add(resultview);

                // intialize the topicview at first, only once
                if (i==1)
                    finalView = new TopicView(forumId,topicId,forumName,topicTitle,postlist);
            }

            // as we have checked topicId exits, sth. else must be wrong in the database
            if (finalView!=null)
                return Result.success(finalView);
            else
                return Result.fatal("finalView uninitialized, some methods in getTopic broke");
       } catch (SQLException e) {
             return Result.fatal(e.getMessage());
       }
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        String q =  " SELECT Topic.topicId AS topicId, Forum.id AS forumId, Forum.title AS forumTitle, " +
                    "   Topic.title AS topicTitle, postCount, created, Post.postedAt as lastPostTime, " +
                    "   author.name AS lastPostName, likes, creator.name AS creatorName, creator.username AS creatorUserName" +
                    " FROM Forum" +
                    " LEFT JOIN Topic ON Topic.forumId = Forum.id " +
                    " LEFT JOIN Post ON Topic.topicId = Post.topicId " +
                    " LEFT JOIN Person author ON author.id = authorId " +
                    " LEFT JOIN Person creator ON creator.id = creatorId " +
                    " LEFT JOIN " +
                    "   ( SELECT Forum.id AS forumId, MAX(postId) AS latest FROM Forum " +
                    "   LEFT JOIN Topic ON Topic.forumId = Forum.id " +
                    "   LEFT JOIN Post ON Topic.topicId = Post.topicId " +
                    "   GROUP BY Forum.id ) AS a ON a.forumId = Forum.id " +
                    " LEFT JOIN " +
                    "   (SELECT Topic.topicId AS topicId,COUNT(*) AS postCount FROM Topic JOIN Post " +
                    "   ON Topic.topicId = Post.topicId GROUP BY Topic.topicId " +
                    "   ) AS c ON Topic.topicId = c.topicId " +
                    " LEFT JOIN " +
                    "   (SELECT topicId, COUNT(*) AS likes FROM PersonLikeTopic GROUP BY topicId " +
                    "   ) AS b ON Topic.topicId = b.topicId " +
                    " WHERE Post.postId = a.latest OR Topic.topicId IS NULL ORDER BY forumTitle ";
        try (PreparedStatement s = c.prepareStatement(q)) {
            ResultSet r = s.executeQuery();

            List<AdvancedForumSummaryView> resultList = new ArrayList<>();
            while (r.next()) {
                int topicId = r.getInt("topicId");
                int forumId = r.getInt("forumId");
                String topicTitle = r.getString("topicTitle");
                String forumTitle = r.getString("forumTitle");
                int postCount = r.getInt("postCount");
                String created = r.getString("created");
                String lastPostTime = r.getString("lastPostTime");
                String lastPostName = r.getString("lastPostName");
                int likes = r.getInt("likes");
                String creatorName = r.getString("creatorName");
                String creatorUserName = r.getString("creatorUserName");

                TopicSummaryView lastTopicView = null;
                if (topicTitle!=null) {
                    lastTopicView = new TopicSummaryView(topicId,forumId,topicTitle,
                        postCount,created,lastPostTime,lastPostName,likes,creatorName,creatorUserName);
                }
                AdvancedForumSummaryView forumView = new AdvancedForumSummaryView(forumId,forumTitle,lastTopicView);
                resultList.add(forumView);
            }
            return Result.success(resultList);
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
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
