-- 新增 extension
create extension ltree;

/* 攤平數據表 */
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
-- 4-1. 上層關係紀錄表
create table hierarchy_relation
(
    id        serial      not null primary key,
    level     integer     not null,
    parent_id integer     not null,
    name      varchar(50) not null
);
insert into hierarchy_relation (parent_id, level, name)
VALUES (0, 0, 'all');

-- 4-2. player 對應 上層關係紀錄表
create table hierarchy_player
(
    id          serial      not null primary key,
    relation_id integer     not null, -- 對應 hierarchy_relation 的 id
    player_name varchar(50) not null
);

-- 4-3. 數據表
create table hierarchy_records
(
    id          bigserial         not null primary key,
    relation_id integer           not null, -- 對應 hierarchy_relation 的 id
    player_id   integer           not null, -- 對應 hierarchy_player 的 id
    balance     integer default 0 not null
);


/* V2 版本, 加入 path 欄位 */
-- 5-1. 上層關係紀錄表
create table hierarchy_relation_ltree
(
    id    serial      not null primary key,
    level integer     not null,
    path  ltree       not null,
    name  varchar(50) not null
);
CREATE INDEX idx_hierarchy_path ON hierarchy_relation_ltree USING GIST (path);
INSERT INTO hierarchy_relation_ltree (path, level, name)
VALUES ('1', 0 'all');

-- 5-2. player 對應 上層關係紀錄表
create table hierarchy_player_ltree
(
    id          serial      not null primary key,
    relation_id integer     not null, -- 對應 hierarchy_relation_ltree 的 id
    player_name varchar(50) not null
);

-- 5-3. 數據表
create table hierarchy_records_ltree
(
    id        bigserial         not null primary key,
    path      ltree             not null, -- 對應 hierarchy_relation_ltree 的 path
    player_id integer           not null, -- 對應 hierarchy_player_ltree 的 id
    balance   integer default 0 not null
);
create index idx_record_path on hierarchy_records_ltree using gist (path);

/* V3 */
create table hierarchy_records_ltree_combine
(
    id        bigserial         not null primary key,
    path      ltree             not null, -- 對應 hierarchy_relation_ltree 的 path
    relation_id integer           not null, -- 對應 hierarchy_player_ltree 的 id
    balance   integer default 0 not null
);