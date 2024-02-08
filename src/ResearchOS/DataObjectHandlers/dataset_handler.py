from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.research_object_handler import ResearchObjectHandler

class DatasetHandler():

    @staticmethod
    def get_addresses(dataset_id: str) -> list:
        """Get all of the addresses in the current dataset."""
        conn = DBConnectionFactory.create_db_connection().conn

        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.        
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE id = '{dataset_id}'"
        action_ids = conn.execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id = action_ids[0] if action_ids else None

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}' AND action_id = '{action_id}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None

        # 2. Get the addresses for the current schema_id.
        sqlquery = f"SELECT action_id FROM data_addresses WHERE schema_id = '{schema_id}'"
        action_ids = conn.execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id = action_ids[0] if action_ids else None

        sqlquery = f"SELECT level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id
          FROM data_addresses WHERE schema_id = '{schema_id}' AND action_id = '{action_id}'"
        result = conn.execute(sqlquery).fetchall()
        # Remove the None components from each row of the result.
        trimmed_result = [dataset_id]
        for row in result:
            trimmed_result.append([x for x in row if x is not None])

        return trimmed_result
    
    @staticmethod
    def set_addresses(addresses: list[list[str]]) -> None:
        """Set the addresses for the current dataset."""
        

        conn = DBConnectionFactory.create_db_connection().conn

        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.        
        dataset_id = addresses[0]
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE id = '{dataset_id}'"
        action_ids = conn.execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id = action_ids[0] if action_ids else None

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}' AND action_id = '{action_id}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None

        # 2. Set the addresses for the current schema_id.        
        # sqlquery = f"INSERT INTO data_addresses (schema_id, action_id, level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id) VALUES (?,
