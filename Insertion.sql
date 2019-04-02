INSERT INTO Person (name, username, stuId) VALUES
   ('David', 'ab12345', NULL),
   ('Steven', 'cd23456', '114514'),
   ('Kerstin', 'ef34567', '87878787');

INSERT INTO Forum (title) VALUES ('Database'), ('Cprogramming'), ('Java');

INSERT INTO Topic (forumId, title, creatorId) VALUES
   (1, 'primarykey', 1),
   (1, 'candidatekey', 2),
   (1, 'entity', 3),
   (2, 'quicksort', 2),
   (2, 'mergesort', 3),
   (2, 'pointer', 1),
   (3, 'inheritance', 3),
   (3, 'OOP', 1),
   (3, 'override', 2);

INSERT INTO Post (topicId, text, authorId) VALUES(1, '1111111111111', 1); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(2, '2222222222222', 2); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(3, '3333333333333', 3); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(4, '4444444444444', 2); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(5, '555555555555555', 3); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(6, '666666666666666', 1); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(7, '777777777777777', 3); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(8, '888888888888888', 1); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(1, '1111111111111', 1); SELECT SLEEP(2);
INSERT INTO Post (topicId, text, authorId) VALUES(9, '999999999999999', 2); SELECT SLEEP(2);

INSERT INTO PersonLikeTopic (id, topicId) VALUES
   (1, 7),
   (1, 8),
   (1, 9),
   (2, 4),
   (2, 5),
   (2, 6),
   (3, 1),
   (3, 2),
   (3, 3);

INSERT INTO PersonLikePost (id, postId) VALUES
   (1, 7),
   (1, 8),
   (1, 9),
   (2, 4),
   (2, 5),
   (2, 6),
   (3, 1),
   (3, 2),
   (3, 3);



