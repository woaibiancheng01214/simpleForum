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
  all functions in B finished!     
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
  * b1 finished
  ## thouroughly tested:

# Chao is working on
  all the B part  

# Li is working on:
  * the parameter "username" in getPersonView method should be checked whether it's exist or not, if not, return Result.failure is required by David in the Lecture (3.25 47:00->47:36), so as all adding/inserting methods (mentioned by wang).
  * Issue about error handling:

  David mentioned in his lecture: success means what you think it does correctly, failure means something went wrong and the responsibility for that is the person who called this method, if you have a method call getPerson() which takes username, and you call it but the username doesn't exist, that's a failure. Fatal is for all kinds of other things like your method throw an SQLException or something goes wrong. Basically, a failure is something went wrong and means __the person who called us should take the responsibility__. Fatal means something else went wrong, which might be the indication of the bug in the code. So you still need to return fatal rather than an exception if somthing like that happens, __one of thoes things mark you are doing correctly__

# Ziteng Wang is working on:
