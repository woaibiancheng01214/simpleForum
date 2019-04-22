-- public Result<Map<String, String>> getUsers()
SELECT username, name FROM Person;


-- public Result<PersonView> getPersonView(String username)
SELECT name, username, stuId FROM Person
WHERE username = ?;


-- public Result addNewPerson(String name, String username, String studentId)
INSERT INTO Person (name, username, studentId) VALUES(?, ?, ?);


-- public Result<List<SimpleForumSummaryView>> getSimpleForums()
SELECT id, title FROM Forum;


-- public Result createForum(String title);
INSERT INTO Forum (title) VALUES(?);


-- public Result<List<ForumSummaryView>> getForums();
SELECT title, id FROM Forum
ORDER BY title ASC;

-- get the latest-Posted-Topic for each Forum
SELECT * FROM
( SELECT topic.forumId, forum.title as forumtitle, topic.topicId, topic.title as topictitle, post.postedAt
    FROM forum JOIN topic ON forum.id = topic.forumId
            JOIN post ON topic.topicId = post.topicId
    ORDER BY forumId DESC, post.postedAt DESC ) AS b
JOIN
( SELECT topic.forumId, MAX(postedAt) as latest
    FROM forum JOIN topic ON forum.id = topic.forumId
            JOIN post ON topic.topicId = post.topicId GROUP BY forumId ) AS a
ON b.forumId = a.forumId AND b.postedAt = a.latest GROUP BY a.forumId;

-- public Result<ForumView> getForum(int id);
SELECT Forum.title AS forum, forumId, topicId, Topic.title AS topic FROM Forum
INNER JOIN Topic ON forumId = id
WHERE id = ?;


-- public Result<SimpleTopicView> getSimpleTopic(int topicId);
-- the postnumber is the order index of each post in the retrieved list
SELECT Topic.topicId AS topicId, title,
Person.name AS author, text, postedAt FROM Topic
INNER JOIN Post ON Topic.topicId = Post.topicId
INNER JOIN Person ON authorId = Person.Id
WHERE Topic.topicId = ? ORDER BY Post.postedAt ASC;

-- public Result<PostView> getLatestPost(int topicId);
-- postNumber is just Count(*), which is the count of the entire joint table
-- as this is the last post
SELECT Topic.forumId AS forum, Post.topicId AS topic,
Person.name AS authorName, Person.username AS authorUserName, text,
COUNT(*) AS postNumber, postedAt, postLike.likes AS likes
FROM Topic INNER JOIN Post ON Topic.topicId = Post.topicId
INNER JOIN Person ON Post.authorId = Person.id
LEFT JOIN (
    SELECT Post.postId AS postId, COUNT(*) AS likes FROM Post
    INNER JOIN PersonLikePost ON PersonLikePost.postId = Post.postId
)AS postLike ON postLike.postId = Post.postId
WHERE Topic.topicId = 1
ORDER BY postedAt DESC
LIMIT 1;


-- createPost
INSERT INTO Post (topicId, text, authorId)
VALUES (? , ?, ( SELECT id FROM Person WHERE Person.username = ? ));


-- createTopic
INSERT INTO Topic (forumId, title, creatorId) VALUES (? ,? , ( SELECT Id FROM Person WHERE Person.username = ? ));
-- plus a java function
createPost(topicId,creatorId,text);


-- countPostsInTopic
SELECT COUNT(*) FROM Post JOIN Topic ON Post.topicId = Topic.topicId WHERE Topic.topicId = ? ;

--B part
SELECT * FROM PersonLikeTopic JOIN Person ON PersonLikeTopic.id = Person.id JOIN Topic ON PersonLikeTopic.topicId
 = Topic.topicId;

SELECT postId
FROM Topic JOIN Post ON Topic.topicId = Post.topicId
WHERE Topic.topicId = ? ORDER BY Post.postedAt ASC LIMIT ?,1 ;

SELECT * FROM PersonLikePost WHERE id = 1 AND postId =
( SELECT postId
FROM Topic JOIN Post ON Topic.topicId = Post.topicId
WHERE Topic.topicId = 12 ORDER BY Post.postedAt ASC LIMIT 0,1 );

-- getTopic (an indeed epic query)
SELECT Topic.topicId AS topicId, Topic.title AS topicTitle,
Forum.id AS forumId, Forum.title AS forumName, Post.text AS postText,Person.name AS authorName, Person.username AS authorUserName,
COUNT(PersonLikePost.id) AS likes, postedAt FROM Topic
JOIN Forum ON Topic.forumId = Forum.id
JOIN Post ON Topic.topicId = Post.topicId
JOIN Person ON Person.id = Post.authorId
LEFT JOIN PersonLikePost ON Post.postId = PersonLikePost.postId
WHERE Topic.topicId = 12 GROUP BY Post.postId ORDER BY postedAt ASC;

-- advancedforums
-- an even more epic query
 SELECT Topic.topicId AS topicId, Forum.id AS forumId, Forum.title AS forumTitle,
 Topic.title AS topicTitle, postCount, created, Post.postedAt as lastPostTime,
 author.name AS lastPostName, likes, creator.name AS creatorName, creator.username AS creatorUserName
 FROM Forum
 LEFT JOIN Topic ON Topic.forumId = Forum.id
 LEFT JOIN Post ON Topic.topicId = Post.topicId
 LEFT JOIN Person author ON author.id = authorId
 LEFT JOIN Person creator ON creator.id = creatorId
 LEFT JOIN
 ( SELECT Forum.id AS forumId, MAX(postedAt) AS latest FROM Forum
 LEFT JOIN Topic ON Topic.forumId = Forum.id
 LEFT JOIN Post ON Topic.topicId = Post.topicId
 GROUP BY Forum.id ) AS a ON a.forumId = Forum.id
 LEFT JOIN
 ( SELECT Topic.topicId AS topicId,COUNT(*) AS postCount FROM Topic JOIN Post
   ON Topic.topicId = Post.topicId GROUP BY Topic.topicId
 ) AS c ON Topic.topicId = c.topicId
 LEFT JOIN
 ( SELECT topicId, COUNT(*) AS likes FROM PersonLikeTopic GROUP BY topicId
)  AS b ON Topic.topicId = b.topicId
 WHERE Post.postedAt = a.latest OR Topic.topicId IS NULL ORDER BY forumTitle;


-- AdvancedPersonView
  SELECT Person.name, Person.username, Person.stuId, topicLikes, postLikes,
  Topic.topicId, Topic.forumId, Topic.title AS topicTitle, postCount, created,
  Post.postedAt as lastPostTime, author.name AS lastPostName, likes,
  creator.name AS creatorName, creator.username AS creatorUserName
  FROM Person
  LEFT JOIN PersonLikeTopic ON Person.id = PersonLikeTopic.id
  LEFT JOIN Topic ON PersonLikeTopic.topicId = Topic.topicId
  LEFT JOIN Post ON Topic.topicId = Post.topicId
  LEFT JOIN Person author ON author.id = Post.authorId
  LEFT JOIN Person creator ON creator.id = Topic.creatorId
  LEFT JOIN
  ( SELECT Topic.topicId AS topicId, MAX(postedAt) AS latest FROM Topic
  LEFT JOIN Post ON Topic.topicId = Post.topicId
  GROUP BY Topic.topicId ) AS a ON a.topicId = Topic.topicId
  LEFT JOIN
  ( SELECT Topic.topicId AS topicId,COUNT(*) AS postCount FROM Topic JOIN Post
    ON Topic.topicId = Post.topicId GROUP BY Topic.topicId
  ) AS b ON Topic.topicId = b.topicId
  LEFT JOIN
  ( SELECT topicId, COUNT(*) AS likes FROM PersonLikeTopic GROUP BY topicId
  )  AS d ON Topic.topicId = d.topicId
  LEFT JOIN
  ( SELECT author.id, COUNT(PersonLikePost.id) AS postLikes
    FROM Post JOIN Person author ON author.id = Post.authorId
    LEFT JOIN PersonlikePost ON Post.postId = PersonLikePost.postId
    WHERE author.username = 'haha'
  )  AS e ON e.id = Person.id
  LEFT JOIN
  ( SELECT creator.id, COUNT(PersonLikeTopic.id) AS topicLikes
    FROM Topic JOIN Person creator ON creator.id = Topic.creatorId
    LEFT JOIN PersonlikeTopic ON Topic.topicId = PersonLikeTopic.topicId
    WHERE creator.username = 'haha'
  ) AS c ON c.id = Person.id
  WHERE Post.postedAt = a.latest AND Person.username = 'haha'
  GROUP BY Person.id, Topic.topicId;

  -- AdvancedForumView
