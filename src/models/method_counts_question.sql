DROP TABLE IF EXISTS method_usage.method_counts_question;
CREATE TABLE method_usage.method_counts_question (
    methods varchar(255) not null,
    count int not null,
	module varchar(255) not null,
	date_added datetime not null
);
