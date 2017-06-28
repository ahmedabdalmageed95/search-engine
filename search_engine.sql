create database search_engine;
use search_engine;
create table search_eng (Importance varchar(45), word varchar(255),url varchar(255),frequency int(11),priority int(11),constraint primary key (Importance,word,url));
create table x (count varchar(255), y int);
insert into x (count,y) values ('last_indexed',0);