# LalalaDB_Java

# Latest Comments:
getSimpleForums() and createForum() is finished.
Minor format fixes 
(There are still some points I have doubts, but I don't know if I should fix all of them in my style and in my branch).3)  

ADVICE: Everybody should check APIProvider and PDF to see   
if the methods you wrote require any parameter checks   
(e.g. I think addNewPerson() needs that). Possibly also those who are responsible of inserting data.  

# function already finished:
  --
  only roughly tested:   
  * public Result<Map<String, String>> getUsers()  
  * public Result<PersonView> getPersonView(String username)  
  * public Result addNewPerson(String name, String username, String studentId)  
  * public Result<List<SimpleForumSummaryView>> getSimpleForums() **(merged)**
  * public Result createForum(String title) **(merged)**  
  --
  thouroughly tested:   
  
# Li is working on:
  * public Result<List<ForumSummaryView>> getForums()  
  * public Result<PostView> getLatestPost(int topicId)

# Ziteng Wang is working on:
  * public Result<ForumView> getForum(int id)
  * public Result<SimpleTopicView> getSimpleTopic(int topicId)
