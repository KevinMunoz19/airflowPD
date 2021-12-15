
CREATE TABLE test.conf(
    id int primary key auto_increment,
    state varchar(256),
    country varchar(256),
    lat decimal(20,5),
    lon decimal(20,5),
    value int,
    date_d varchar(256)
);

CREATE TABLE test.recovered(
    id int primary key auto_increment,
    state varchar(256),
    country varchar(256),
    lat decimal(20,5),
    lon decimal(20,5),
    value int,
    date_d varchar(256)
);

CREATE TABLE test.deaths(
    id int primary key auto_increment,
    state varchar(256),
    country varchar(256),
    lat decimal(20,5),
    lon decimal(20,5),
    value int,
    date_d varchar(256)
);