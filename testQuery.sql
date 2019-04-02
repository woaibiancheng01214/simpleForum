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
INSERT INTO Topic ( topicId, forumId, title, creatorId) VALUES (? ,? ,? , ( SELECT Id FROM Person WHERE Person.username = ? ));
-- plus a java function
createPost(topicId,creatorId,text);


-- countPostsInTopic
SELECT COUNT(*) FROM Post JOIN Topic ON Post.topicId = Topic.topicId WHERE Topic.topicId = ? ;
