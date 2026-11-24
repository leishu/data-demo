-- 迁移 V2b：回填 channel（CoW 会重写受影响的数据文件）。
UPDATE ${silver_table} SET channel = CASE PMOD(HASH(user_id), 3)
WHEN 0 THEN 'APP' WHEN 1 THEN 'MINI_APP' ELSE 'WEB' END;
