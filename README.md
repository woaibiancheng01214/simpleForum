# LalalaDB_Java

# Latest Comments:
getSimpleForums() and createForum() is finished.
Minor format fixes 
(There are still some points I have doubts, but I don't know if I should fix all of them in my style and in my branch).3)  

ADVICE: Everybody should check APIProvider and PDF to see   
if the methods you wrote require any parameter checks   
(e.g. I think addNewPerson() needs that). Possibly also those who are responsible of inserting data.  

# function already finished:
  all functions in A are finished!     
  ## only roughly tested:   
  * public Result<Map<String, String>> getUsers()  
  * public Result<PersonView> getPersonView(String username)  
  * public Result addNewPerson(String name, String username, String studentId)  
  * public Result<List<SimpleForumSummaryView>> getSimpleForums() **(merged)**
  * public Result createForum(String title) **(merged)**  
  * public Result<List<ForumSummaryView>> getForums()  **(merged)** 
  * public Result<ForumView> getForum(int id) **(merged)** 
  * public Result<SimpleTopicView> getSimpleTopic(int topicId) **(merged)** 
  * public Result<PostView> getLatestPost(int topicId) **(merged)** 
  * public Result createPost(int topicId, String username, String text);
  * public Result createTopic(int forumId, String username, String title, String text);
  * public Result<Integer> countPostsInTopic(int topicId);
  ## thouroughly tested:   
  
# Li is working on:

# Ziteng Wang is working on:
