DROP TABLE IF EXISTS method_usage.method_counts;
CREATE TABLE method_usage.method_counts (
    methods varchar(255) not null,
    count int not null,
	module varchar(255) not null,
	date_added datetime not null
)
