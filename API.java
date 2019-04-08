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
            return tryRollback(e);
        }

        return result;
    }

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
            return tryRollback(e);
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
        if (username == null || text == null) {
            return Result.failure("username or text can not be NULL!");
        }
        if (username.equals("") || text.equals("")) {
            return Result.failure("Author's username or Post's text can not be empty!");
        }

        // topicId checking
        Result topicIdCheck = checkTopicId(topicId);
        if(!topicIdCheck.isSuccess()) return topicIdCheck;

        // fetch userId and check
        Result<Integer> userIdResult = getUserId(username);
        if(!userIdResult.isSuccess()){
           return userIdResult;
        }

        int authorId = userIdResult.getValue().intValue();
        return createPostFromUserId(topicId,authorId,text);
    }

    private Result createPostFromUserId(int topicId, int authorId, String text) {
        Result result = null;
      // everything is already checked in previous function
        try {
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
            return tryRollback(e);
        }
        if(result.isSuccess()) System.out.println("createPost(FromUserId) Function successfully executed!");
        return result;
    }

    private Result<Integer> getUserId(String username){
        Integer userId = null;
        Result<Integer> result = null;
        try {
            PreparedStatement s0 = c.prepareStatement(
               "SELECT id FROM Person WHERE Person.username = ?"
            );
            s0.setString(1,username);
            ResultSet r = s0.executeQuery();
            if(r.next()){
               userId = Integer.valueOf(r.getInt("id"));
               result = Result.success(userId);
            }
            else result = Result.failure("There is no such username");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return result;
    }

    private Result checkTopicId(int topicId)
    {   Result result = null;
        try {
            PreparedStatement s0 = c.prepareStatement(
               "SELECT * FROM Topic WHERE topicId = ?"
            );
            s0.setInt(1,topicId);
            ResultSet r = s0.executeQuery();
            if(r.next()){
                result = Result.success();
            }
            else result = Result.failure("There is no such topicId");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return result;
    }

    private Result checkForumId(int forumId)
    {   Result result = null;
        try {
            PreparedStatement s0 = c.prepareStatement(
               "SELECT * FROM Forum WHERE id = ?"
            );
            s0.setInt(1,forumId);
            ResultSet r = s0.executeQuery();
            if(r.next()){
                result = Result.success();
            }
            else result = Result.failure("There is no such forumId");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return result;
    }

    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        Result result = null;
        if (username == null || text == null || title == null) {
            return Result.failure("username or text or topic title can not be NULL!");
        }
        if (username.equals("") || text.equals("") || title.equals("")) {
            return Result.failure("Author's username or initial Post's text or topic's title can not be empty!");
        }

        // forumId checking
        Result forumIdCheck = checkForumId(forumId);
        if(!forumIdCheck.isSuccess()) return forumIdCheck;

        //fetch userId and checking
        Result<Integer> userIdResult = getUserId(username);
        if(!userIdResult.isSuccess()){  return userIdResult;}

        try {
            int creatorId = userIdResult.getValue().intValue();

            PreparedStatement s = c.prepareStatement(
                "INSERT INTO Topic (forumId, title, creatorId) VALUES (? ,? , ?) "
            );
            s.setInt(1,forumId);
            s.setString(2,title);
            s.setInt(3,creatorId);

            s.executeUpdate();
            result = Result.success();
            s.close();

            // potential concurrency problem with these two queries
            // maybe use insert and fetch method with one query?
            c.commit();

            int topicId;
            try{
               PreparedStatement s1 = c.prepareStatement(
                   "SELECT LAST_INSERT_ID();"
               );
               ResultSet r = s1.executeQuery();
               if(r.next()) topicId = r.getInt("LAST_INSERT_ID()");
               else return Result.fatal("function --- LAST_INSERT_ID() in mariadb failed");
            } catch(SQLException e) { return Result.fatal(e.getMessage()); }

            // using function createPostFromUserId() to save one getUserId query
            Result firstPostResult = createPostFromUserId(topicId,creatorId,text);
            if(!firstPostResult.isSuccess()) return firstPostResult;

        } catch (SQLException e) {
            return tryRollback(e);
        }
        if(result.isSuccess()) System.out.println("createTopic Function successfully executed!");
        return result;
    }

   /* as mentioned in the offical documentation
       this function is never used on web interface, therefore not tested yet*/
    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        Integer count = null;
        Result<Integer> result = null;
        try {
            PreparedStatement s0 = c.prepareStatement(
               "SELECT COUNT(*) FROM Post JOIN Topic ON Post.topicId = Topic.topicId WHERE Topic.topicId = ? "
            );
            s0.setInt(1,topicId);
            ResultSet r = s0.executeQuery();
            if(r.next()){
               count = Integer.valueOf(r.getInt("id"));
               result = Result.success(count);
            }
            else return Result.failure("There is no such topic with this topicId");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }
        return result;
    }

    /* B.1 */
      // the situation topicId fails the foreign key constrain should return
      // failure. currently it will fall into SQLException and fatal
    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
        if(username==null || username.equals(""))
        return Result.failure("username can not be null");

        // check topicId
        Result topicIdCheck = checkTopicId(topicId);
        if(!topicIdCheck.isSuccess()) return topicIdCheck;

        // get user's id and check
        Result<Integer> userIdResult = getUserId(username);
        if(!userIdResult.isSuccess()) return userIdResult;

        Result result = null;
        int userId = userIdResult.getValue().intValue();
        try {
            PreparedStatement s0 = c.prepareStatement(
               "SELECT * FROM PersonLikeTopic WHERE topicId = ? AND id = ? "
            );
            s0.setInt(1,topicId);
            s0.setInt(2,userId);
            ResultSet r = s0.executeQuery();

            // situation judge
            // if already like/unlike still return success as instructed
            if(r.next()){
                if(!like) result = deleteLikeTopic(userId,topicId);
                else result = Result.success();
            }
            else
            {   if(!like) result = Result.success();
                else result = addLikeTopic(userId,topicId);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    private Result addLikeTopic(int userId, int topicId)
    {   Result result = null;
        try {
            PreparedStatement s = c.prepareStatement(
                "INSERT INTO PersonLikeTopic (id,topicId) VALUES ( ? , ? )"
            );
            s.setInt(1,userId);
            s.setInt(2,topicId);
            s.executeUpdate();
            result = Result.success();
            s.close();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        if(result.isSuccess()) System.out.println("addLikeTopic(liketopic) Function successfully executed!");
        return result;
    }

    private Result deleteLikeTopic(int userId, int topicId)
    {   Result result = null;
        try {
            PreparedStatement s = c.prepareStatement(
                "DELETE FROM PersonLikeTopic WHERE id = ? AND topicId = ?"
            );
            s.setInt(1,userId);
            s.setInt(2,topicId);
            s.executeUpdate();
            result = Result.success();
            s.close();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        if(result.isSuccess()) System.out.println("deleteLikeTopic(liketopic) Function successfully executed!");
        return result;
    }


    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
         if(username==null || username.equals(""))
        return Result.failure("username can not be null");
        if(post<1) return Result.failure("postNumber should be bigger than or equal to one");

        // check topicId
        Result topicIdCheck = checkTopicId(topicId);
        if(!topicIdCheck.isSuccess()) return topicIdCheck;

        // get user's id and check
        Result<Integer> userIdResult = getUserId(username);
        if(!userIdResult.isSuccess()) return userIdResult;

        Result result = null;
        int userId = userIdResult.getValue().intValue();
        int postId;

        //get postId
        try {
            PreparedStatement s0 = c.prepareStatement(
                 " SELECT postId FROM Topic JOIN Post ON Topic.topicId = Post.topicId " +
                 " WHERE Topic.topicId = ? ORDER BY Post.postedAt ASC LIMIT ?,1 "
            );
            s0.setInt(1,topicId);
            s0.setInt(2,post-1);
            ResultSet r = s0.executeQuery();
            if(r.next()) postId = r.getInt("postId");
            else return Result.failure("this post doesn't exit");
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        // to like or unlike a post
        try {
            PreparedStatement s1 = c.prepareStatement(
               "SELECT * FROM PersonLikePost WHERE id = ? AND postId = ?"
            );
            s1.setInt(1,userId);
            s1.setInt(2,postId);
            ResultSet r = s1.executeQuery();

            // situation judge
            // if already like/unlike still return success as instructed
            if(r.next()){
                if(!like) result = deleteLikePost(userId,postId);
                else result = Result.success();
            }
            else
            {   if(!like) result = Result.success();
                else result = addLikePost(userId,postId);
            }
        } catch (SQLException e) {
            return Result.fatal(e.getMessage());
        }

        return result;
    }

    private Result addLikePost(int userId, int postId)
    {   Result result = null;
        try {
            PreparedStatement s = c.prepareStatement(
                "INSERT INTO PersonLikePost (id,postId) VALUES ( ? , ? )"
            );
            s.setInt(1,userId);
            s.setInt(2,postId);
            s.executeUpdate();
            result = Result.success();
            s.close();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        if(result.isSuccess()) System.out.println("addLikePost(likePost) Function successfully executed!");
        return result;
    }

    private Result deleteLikePost(int userId, int postId)
    {   Result result = null;
        try {
            PreparedStatement s = c.prepareStatement(
                "DELETE FROM PersonLikePost WHERE id = ? AND postId = ?"
            );
            s.setInt(1,userId);
            s.setInt(2,postId);
            s.executeUpdate();
            result = Result.success();
            s.close();
            c.commit();
        } catch (SQLException e) {
            return tryRollback(e);
        }
        if(result.isSuccess()) System.out.println("deleteLikePost(likePost) Function successfully executed!");
        return result;
    }


    // **********this function is not in the web user iterface therefore not tested
    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        Result<List<PersonView>> result = null;

        // check whether topicId exists
        Result topicIdCheck = checkTopicId(topicId);
        if(!topicIdCheck.isSuccess()) return topicIdCheck;

        try {
            PreparedStatement s0 = c.prepareStatement(
               " SELECT * FROM PersonLikeTopic JOIN Person ON PersonLikeTopic.id = Person.id " +
               " WHERE topicId = ? "
            );
            s0.setInt(1,topicId);
            ResultSet r = s0.executeQuery();

            // return success even if it is an empty list
            List<PersonView> resultlist = new ArrayList<>();
            while(r.next()){
                String name = r.getString("name");
                String stuId = r.getString("stuId");
                if (stuId == null) stuId = "null";
                String username = r.getString("username");
                PersonView resultview = new PersonView(name,username,stuId);
                resultlist.add(resultview);
            }
            result = Result.success(resultlist);
       } catch (SQLException e) {
             return Result.fatal(e.getMessage());
       }

       return result;
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        //topicId checking existance
        Result topicIdCheck = checkTopicId(topicId);
        if(!topicIdCheck.isSuccess()) return topicIdCheck;

        Result<TopicView> result = null;
        try {
            PreparedStatement s0 = c.prepareStatement(
                   "SELECT Topic.topicId AS topicId, Topic.title AS topicTitle, " +
                   "Forum.id AS forumId, Forum.title AS forumName, Post.text AS postText, " +
                   "Person.name AS authorName, Person.username AS authorUserName, " +
                   "COUNT(PersonLikePost.id) AS likes, postedAt FROM Topic " +
                   "JOIN Forum ON Topic.forumId = Forum.id " +
                   "JOIN Post ON Topic.topicId = Post.topicId "+
                   "JOIN Person ON Person.id = Post.authorId "+
                   "LEFT JOIN PersonLikePost ON Post.postId = PersonLikePost.postId "+
                   "WHERE Topic.topicId = ? GROUP BY Post.postId ORDER BY postedAt ASC"
            );
            s0.setInt(1,topicId);
            ResultSet r = s0.executeQuery();

            List<PostView> postlist = new ArrayList<>();
            TopicView finalView = null;
            for(int i=1;r.next();i++){
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
                if(i==1)
                {   finalView = new TopicView(forumId,topicId,forumName,topicTitle,postlist);
                }
            }

            // as we have checked topicId exits, sth. else must be wrong in the database
            if(finalView!=null) result = Result.success(finalView);
            else return Result.fatal("finalView uninitialized, some methods in getTopic broke");
       } catch (SQLException e) {
             return Result.fatal(e.getMessage());
       }
       return result;
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        Result<List<AdvancedForumSummaryView>> result = null;
        try {
            PreparedStatement s0 = c.prepareStatement(
               " SELECT Topic.topicId AS topicId, Forum.id AS forumId, Forum.title AS forumTitle, " +
               " Topic.title AS topicTitle, postCount, created, Post.postedAt as lastPostTime, " +
               " author.name AS lastPostName, likes, creator.name AS creatorName, creator.username AS creatorUserName" +
               " FROM Forum" +
               " LEFT JOIN Topic ON Topic.forumId = Forum.id " +
               " LEFT JOIN Post ON Topic.topicId = Post.topicId " +
               " LEFT JOIN Person author ON author.id = authorId " +
               " LEFT JOIN Person creator ON creator.id = creatorId " +
               " LEFT JOIN " +
               " ( SELECT Forum.id AS forumId, MAX(postedAt) AS latest FROM Forum " +
               " LEFT JOIN Topic ON Topic.forumId = Forum.id " +
               " LEFT JOIN Post ON Topic.topicId = Post.topicId " +
               " GROUP BY Forum.id ) AS a ON a.forumId = Forum.id " +
               " LEFT JOIN " +
               " ( SELECT Topic.topicId AS topicId,COUNT(*) AS postCount FROM Topic JOIN Post " +
               "   ON Topic.topicId = Post.topicId GROUP BY Topic.topicId " +
               " ) AS c ON Topic.topicId = c.topicId " +
               " LEFT JOIN " +
               " (  SELECT topicId, COUNT(*) AS likes FROM PersonLikeTopic GROUP BY topicId " +
               " )  AS b ON Topic.topicId = b.topicId " +
               " WHERE Post.postedAt = a.latest OR Topic.topicId IS NULL ORDER BY forumTitle "
            );
            ResultSet r = s0.executeQuery();

            List<AdvancedForumSummaryView> resultList = new ArrayList<>();
            while(r.next())
            {   int topicId = r.getInt("topicId");
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
                if(topicTitle!=null)
                {   lastTopicView = new TopicSummaryView(topicId,forumId,topicTitle,
                        postCount,created,lastPostTime,lastPostName,likes,creatorName,creatorUserName);
                }
                AdvancedForumSummaryView forumView = new AdvancedForumSummaryView(forumId,forumTitle,lastTopicView);
                resultList.add(forumView);
            }
            result = Result.success(resultList);
       } catch (SQLException e) {
             return Result.fatal(e.getMessage());
       }
       return result;
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
         Result<AdvancedPersonView> result = null;
         try {
              PreparedStatement s0 = c.prepareStatement(
                 " SELECT Person.name, Person.username, Person.stuId, topicLikes, postLikes," +
                 " Topic.topicId, Topic.forumId, Topic.title AS topicTitle, postCount, created," +
                 " Post.postedAt as lastPostTime, author.name AS lastPostName, likes," +
                 " creator.name AS creatorName, creator.username AS creatorUserName" +
                 " FROM Person " +
                 "  LEFT JOIN PersonLikeTopic ON Person.id = PersonLikeTopic.id" +
                 " LEFT JOIN Topic ON PersonLikeTopic.topicId = Topic.topicId" +
                 " LEFT JOIN Post ON Topic.topicId = Post.topicId" +
                 " LEFT JOIN Person author ON author.id = Post.authorId" +
                 " LEFT JOIN Person creator ON creator.id = Topic.creatorId" +
                 " LEFT JOIN" +
                 " ( SELECT Topic.topicId AS topicId, MAX(postedAt) AS latest FROM Topic" +
                 " LEFT JOIN Post ON Topic.topicId = Post.topicId " +
                 " GROUP BY Topic.topicId ) AS a ON a.topicId = Topic.topicId" +
                 " LEFT JOIN" +
                 " ( SELECT Topic.topicId AS topicId,COUNT(*) AS postCount FROM Topic JOIN Post" +
                 " ON Topic.topicId = Post.topicId GROUP BY Topic.topicId" +
                 " ) AS b ON Topic.topicId = b.topicId" +
                 " LEFT JOIN" +
                 " ( SELECT topicId, COUNT(*) AS likes FROM PersonLikeTopic GROUP BY topicId " +
                 " )  AS d ON Topic.topicId = d.topicId" +
                 " LEFT JOIN" +
                 " ( SELECT author.id, COUNT(PersonLikePost.id) AS postLikes" +
                 "  FROM Post JOIN Person author ON author.id = Post.authorId" +
                 " LEFT JOIN PersonlikePost ON Post.postId = PersonLikePost.postId" +
                 " WHERE author.username = ?" +
                 "   )  AS e ON e.id = Person.id"+
                 " LEFT JOIN"+
                 " ( SELECT creator.id, COUNT(PersonLikeTopic.id) AS topicLikes"+
                 " FROM Topic JOIN Person creator ON creator.id = Topic.creatorId"+
                 " LEFT JOIN PersonlikeTopic ON Topic.topicId = PersonLikeTopic.topicId"+
                 " WHERE creator.username = ? "+
                 "   ) AS c ON c.id = Person.id"+
                 " WHERE Post.postedAt = a.latest AND Person.username = ? "+
                 " GROUP BY Person.id, Topic.topicId"
              );
              s0.setString(1,username);
              s0.setString(2,username);
              s0.setString(3,username);
              ResultSet r = s0.executeQuery();
              AdvancedPersonView finalView = null;
              List<TopicSummaryView> topicList = new ArrayList<>();
              for(int i = 0;r.next();i++)
              {   int topicId = r.getInt("topicId");
                  int forumId = r.getInt("forumId");
                  String topicTitle = r.getString("topicTitle");
                  int postCount = r.getInt("postCount");
                  String created = r.getString("created");
                  String lastPostTime = r.getString("lastPostTime");
                  String lastPostName = r.getString("lastPostName");
                  int likes = r.getInt("likes");
                  String creatorName = r.getString("creatorName");
                  String creatorUserName = r.getString("creatorUserName");

                  TopicSummaryView lastTopicView = null;
                  if(topicTitle!=null)
                  {   lastTopicView = new TopicSummaryView(topicId,forumId,topicTitle,
                      postCount,created,lastPostTime,lastPostName,likes,creatorName,creatorUserName);
                  }
                  topicList.add(lastTopicView);

                  // init in beginning
                  if(i==0)
                  {   String name = r.getString("name");
                      String stuId = r.getString("stuId");
                      int topicLikes = r.getInt("topicLikes");
                      int postLikes = r.getInt("postLikes");
                      finalView = new AdvancedPersonView(name,username,stuId,topicLikes,
                            postLikes, topicList);
                  }
              }
              result = Result.success(finalView);
         } catch (SQLException e) {
              return Result.fatal(e.getMessage());
         }
         if(result.isSuccess()) System.out.print("hahahahahahaha, AdvancedPersonView successsfully executed");
         return result;
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
