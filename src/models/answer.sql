DROP TABLE IF EXISTS test.pandas_answer;
CREATE TABLE test.pandas_answer (
	answer_id int not null primary key,
	question_id int not null,
	owner int not null,
	last_activity_date datetime not null,
	creation_date datetime not null,
	link varchar(510),
	body varchar(max),
	last_edit_date datetime,
	date_added datetime
);
