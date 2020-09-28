DROP TABLE IF EXISTS test.method_counts_question;
CREATE TABLE test.method_counts_question (
    methods varchar(255) not null,
    count int not null,
	module varchar(255) not null,
	date_added datetime not null
);
