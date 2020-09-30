DROP TABLE IF EXISTS test.pandas_question;
CREATE TABLE test.pandas_question (
	question_id INT NOT NULL PRIMARY KEY,
	tags VARCHAR(255),
	owner INT NOT NULL,
	is_answered BIT,
	answer_count INT,
	score INT,
	last_activity_date DATETIME NOT NULL,
	creation_date DATETIME NOT NULL,
	link VARCHAR(510),
	title VARCHAR(510),
	body VARCHAR(max),
	last_edit_date DATETIME,
	accepted_answer_id DECIMAL(18,1),
	date_added DATETIME
);
