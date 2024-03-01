-- SQLite
SELECT action_id, attr_id, attr_value                
FROM (
    SELECT simple_attributes.action_id, simple_attributes.attr_id, simple_attributes.attr_value, 
    ROW_NUMBER() OVER (PARTITION BY simple_attributes.object_id ORDER BY actions.datetime DESC) AS row_num
    FROM simple_attributes
    JOIN actions ON simple_attributes.action_id = actions.action_id
    WHERE simple_attributes.object_id = ?
    ORDER BY actions.datetime DESC
) AS ranked
WHERE row_num = 1;

SELECT * FROM simple_attributes