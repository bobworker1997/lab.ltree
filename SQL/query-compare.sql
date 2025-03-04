/* Ltree vs CTE */
-- V2
WITH RECURSIVE tree AS (
    -- 起始節點
    SELECT id, parent_id, level, name
    FROM hierarchy_relation
    WHERE name = 'Company1'
      AND level = 1

    UNION ALL

    -- 遞歸查詢所有子節點
    SELECT p.id, p.parent_id, p.level, p.name
    FROM hierarchy_relation p
             JOIN tree t ON p.parent_id = t.id)
SELECT *
FROM tree
WHERE name != 'Company1';

-- V3
SELECT id,
       path,
       name
FROM hierarchy_relation_ltree
WHERE path <@ (SELECT path FROM hierarchy_relation_ltree WHERE name = 'Company1')
                                                           AND name != 'Company1';

/* V0 - 攤平 */
    
-- case1: all company
select company, sum(balance) as total
from flattening_records
group by company;
-- case2: all sub_system by some company
select sub_system, sum(balance) as total
from flattening_records
where company = 'Company1'
group by company, sub_system;
-- case3: all sub_system by some company & sub_system
select web_id, sum(balance) as total
from flattening_records
where company = 'Company1' and sub_system = 'Subsystem1'
group by company, sub_system, web_id;
-- case4: all player by some company & sub_system & web_id
select player_name, sum(balance) as total
from flattening_records
where company = 'Company1' and sub_system = 'Subsystem1' and web_id = 'WebId1'
group by player_name;


/* V1 - 未加入 ltree  */
-- case1: all company
WITH company_relations AS (
    SELECT r1.id, r1.name
    FROM hierarchy_relation r1
    WHERE parent_id IN (select id from hierarchy_relation where name = 'all' and parent_id = 0)
)
SELECT
    cr.name AS company,
    COALESCE(SUM(hrr.balance), 0) AS total_balance
FROM company_relations cr
         LEFT JOIN hierarchy_relation r2 ON r2.parent_id = cr.id
         LEFT JOIN hierarchy_relation r3 ON r3.parent_id = r2.id
         LEFT JOIN hierarchy_records hrr ON hrr.relation_id = r3.id
GROUP BY cr.name;
-- case2: all sub_system by some company
WITH company_relation AS (
    SELECT id
    FROM hierarchy_relation
    WHERE level = 1 AND name = 'Company1'
),
     subsystem_relations AS (
         SELECT r.id, r.name
         FROM hierarchy_relation r
                  JOIN company_relation cr ON r.parent_id = cr.id
         WHERE r.level = 2
     )
SELECT
    sr.name AS sub_system,
    COALESCE(SUM(hrr.balance), 0) AS total_balance
FROM subsystem_relations sr
         LEFT JOIN hierarchy_relation r ON r.parent_id = sr.id
         LEFT JOIN hierarchy_records hrr ON hrr.relation_id = r.id
GROUP BY sr.name;
-- case3: all sub_system by some company & sub_system
WITH target_path AS (
    SELECT r3.id, r3.name
    FROM hierarchy_relation r1
             JOIN hierarchy_relation r2 ON r2.parent_id = r1.id
             JOIN hierarchy_relation r3 ON r3.parent_id = r2.id
    WHERE r1.level = 1 AND r1.name = 'Company1'
      AND r2.level = 2 AND r2.name = 'Subsystem1'
      AND r3.level = 3
)
SELECT
    tp.name AS web_id,
    COALESCE(SUM(hrr.balance), 0) AS total_balance
FROM target_path tp
         LEFT JOIN hierarchy_records hrr ON hrr.relation_id = tp.id
GROUP BY tp.name;
-- case4: all player by some company & sub_system & web_id
WITH target_path AS (
    SELECT r3.id, r3.name
    FROM hierarchy_relation r1
             JOIN hierarchy_relation r2 ON r2.parent_id = r1.id
             JOIN hierarchy_relation r3 ON r3.parent_id = r2.id
    WHERE r1.level = 1 AND r1.name = 'Company1'
      AND r2.level = 2 AND r2.name = 'Subsystem1'
      AND r3.level = 3 AND r3.name = 'WebId1'
)
SELECT
    hp.player_name,
    COALESCE(SUM(hrr.balance), 0) AS total_balance
FROM target_path tp
         LEFT JOIN hierarchy_records hrr ON hrr.relation_id = tp.id
         LEFT JOIN hierarchy_player hp ON hp.id = hrr.player_id
GROUP BY hp.player_name;

/* V2 - 加入 ltree 欄位 */
-- case1: all company
SELECT
    lhr.name AS company,
    SUM(lhrr.balance) AS total_balance
FROM hierarchy_relation_ltree lhr
         JOIN hierarchy_records_ltree lhrr ON lhrr.path <@ lhr.path
WHERE lhr.path ~ '1.*{1}'
GROUP BY lhr.name;
-- case2: all sub_system by some company
SELECT
    lhr.name AS sub_system,
    SUM(lhrr.balance) AS total_balance
FROM hierarchy_relation_ltree lhr
         JOIN hierarchy_records_ltree lhrr ON lhrr.path <@ lhr.path
WHERE lhr.path ~ '1.2.*{1}'
GROUP BY lhr.name;
-- case3: all sub_system by some company & sub_system
SELECT
    lhr.name AS web_id,
    SUM(lhrr.balance) AS total_balance
FROM hierarchy_relation_ltree lhr
         JOIN hierarchy_records_ltree lhrr ON lhrr.path <@ lhr.path
WHERE lhr.path ~ '1.2.3.*{1}'
GROUP BY lhr.name
ORDER BY name;
-- case4: all player by some company & sub_system & web_id
SELECT
    (SELECT player_name FROM hierarchy_player_ltree WHERE id = hr.player_id) AS player_name,
    SUM(balance) AS total_balance
FROM hierarchy_records_ltree hr
WHERE hr.path = (
    SELECT path
    FROM hierarchy_relation_ltree
    WHERE name = 'WebId1'  -- 替換為 Web ID
      AND path <@ (
SELECT path
FROM hierarchy_relation_ltree
WHERE name = 'Subsystem1'  -- 替換為子系統名稱
  AND path <@ (
SELECT path
FROM hierarchy_relation_ltree
WHERE name = 'Company1'  -- 替換為公司名稱
    )
      )
)
GROUP BY player_id;

/* 執行計劃 */
-- V0
EXPLAIN select player_name, sum(balance) as total
        from flattening_records
        where company = 'Company1' and sub_system = 'Subsystem1' and web_id = 'WebId1'
        group by player_name;
-- V1
EXPLAIN WITH target_path AS (
    SELECT r3.id, r3.name
    FROM hierarchy_relation r1
    JOIN hierarchy_relation r2 ON r2.parent_id = r1.id
    JOIN hierarchy_relation r3 ON r3.parent_id = r2.id
    WHERE r1.level = 1 AND r1.name = 'Company1'
    AND r2.level = 2 AND r2.name = 'Subsystem1'
    AND r3.level = 3 AND r3.name = 'WebId1'
)
SELECT
    hp.player_name,
    COALESCE(SUM(hrr.balance), 0) AS total_balance
FROM target_path tp
         LEFT JOIN hierarchy_records hrr ON hrr.relation_id = tp.id
         LEFT JOIN hierarchy_player hp ON hp.id = hrr.player_id
GROUP BY hp.player_name;
-- V2
EXPLAIN
SELECT
    (SELECT player_name FROM hierarchy_player_ltree WHERE id = hr.player_id) AS player_name,
    SUM(balance) AS total_balance
FROM hierarchy_records_ltree hr
WHERE hr.path = (
    SELECT path
    FROM hierarchy_relation_ltree
    WHERE name = 'WebId1'  -- 替換為 Web ID
      AND path <@ (
SELECT path
FROM hierarchy_relation_ltree
WHERE name = 'Subsystem1'  -- 替換為子系統名稱
  AND path <@ (
SELECT path
FROM hierarchy_relation_ltree
WHERE name = 'Company1'  -- 替換為公司名稱
    )
      )
)
GROUP BY player_id;