-- 新增 extension
create extension ltree;

-- 原始結構的數據表
create table flattening_records
(
    id          bigserial   not null primary key,
    company     varchar(50) not null,
    sub_system  varchar(50) not null,
    web_id      varchar(50) not null,
    player_name varchar(50) not null,
    balance     integer     not null
);


/* V1 版本, 未加入 path 欄位 */
create table hierarchy_relation
(
    id   serial      not null primary key,
    parent_id integer not null,
    name varchar(50) not null
);
insert into hierarchy_relation (parent_id, name) VALUES (0, 'all');

create table hierarchy_player
(
    id          serial      not null primary key,
    relation_id integer     not null references hierarchy_relation (id),
    player_name varchar(50) not null
);
create table hierarchy_records
(
    id        bigserial         not null primary key,
    relation_id integer     not null references hierarchy_relation (id),
    player_id integer           not null,
    balance   integer default 0 not null
);

/* V2 版本, 加入 path 欄位 */
create table hierarchy_relation_ltree
(
    id   serial      not null primary key,
    path ltree       not null,
    name varchar(50) not null
);
CREATE INDEX idx_hierarchy_path ON hierarchy_relation_ltree USING GIST (path);
INSERT INTO hierarchy_relation_ltree (path, name) VALUES ('1', 'all');

create table hierarchy_player_ltree
(
    id          serial      not null primary key,
    relation_id integer     not null references hierarchy_relation_ltree (id),
    player_name varchar(50) not null
);
create table hierarchy_records_ltree
(
    id        bigserial         not null primary key,
    path      ltree             not null,
    player_id integer           not null, -- references hierarchy_player_ltree (id),
    balance   integer default 0 not null
);
create index idx_record_path on hierarchy_records_ltree using gist (path);