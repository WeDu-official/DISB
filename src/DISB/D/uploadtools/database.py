import sqlite3
import os
import traceback
from typing import Dict, Any, List
import asyncio
class DatabaseManager:
    def __init__(self, file_table_columns: List[str], log):
        """
        file_table_columns: list of 18 column names (fileid, base_filename, etc.)
        log: logger instance
        """
        self.file_table_columns = file_table_columns
        self.log = log

    def _sync_create_table_if_not_exists(self, conn: sqlite3.Connection):
        column_definitions = []
        for col_name in self.file_table_columns:
            if col_name == "fileid":
                column_definitions.append(f"{col_name} TEXT PRIMARY KEY")
            elif col_name in ["part_number", "total_parts", "message_id", "channel_id"]:
                column_definitions.append(f"{col_name} INTEGER")
            elif col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                column_definitions.append(f"{col_name} INTEGER")  # Store as INTEGER 0 or 1 for boolean
            elif col_name == "encryption_key_auto":
                column_definitions.append(f"{col_name} BLOB")  # Store as BLOB for bytes
            else:
                column_definitions.append(f"{col_name} TEXT")
        schema = ", ".join(column_definitions)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS file_metadata_table (
            {schema}
        );
        """
        conn.execute(create_table_sql)
        conn.commit()
        self.log.info(f"[SQLite] Ensured table 'file_metadata_table' exists synchronously.")


    def _sync_insert_operation(self, database_file: str, entry: Dict[str, Any]):
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        os.makedirs(os.path.dirname(db_path_abs), exist_ok=True)
        conn = None
        try:
            print(db_path_abs + ": INSERT")
            conn = sqlite3.connect(db_path_abs)
            conn.row_factory = sqlite3.Row
            self._sync_create_table_if_not_exists(conn)
            columns = []
            placeholders = []
            values = []
            for col_name in self.file_table_columns:
                value_to_store = entry.get(col_name)
                # Handle default values for None
                if value_to_store is None:
                    if col_name in ["part_number", "total_parts", "message_id", "channel_id"]:
                        value_to_store = 0  # Store as integer 0
                    elif col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                        value_to_store = 0  # Store as integer 0 for False
                    elif col_name == "encryption_key_auto":
                        value_to_store = b""  # Store as empty bytes
                    else:
                        value_to_store = ""  # Default for other TEXT columns

                # Type conversion for database storage
                if col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                    value_to_store = 1 if value_to_store else 0  # Convert bool to int (1/0)
                elif col_name == "encryption_key_auto":
                    # Ensure it's bytes for BLOB, convert from str if necessary (e.g., if loaded from DB as str)
                    if isinstance(value_to_store, str):
                        value_to_store = value_to_store.encode('utf-8')
                elif col_name in ["part_number", "total_parts", "message_id", "channel_id"]:
                    value_to_store = int(value_to_store)  # Ensure it's int
                else:
                    value_to_store = str(value_to_store)  # Store everything else as TEXT

                columns.append(col_name)
                placeholders.append("?")
                values.append(value_to_store)
            insert_sql = f"""
            INSERT OR REPLACE INTO file_metadata_table ({", ".join(columns)})
            VALUES ({", ".join(placeholders)});
            """
            conn.execute(insert_sql, tuple(values))
            conn.commit()
            self.log.info(
                f"[SQLite] Successfully inserted new entry with fileid '{entry.get('fileid')}' into '{database_file}'.")
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"[SQLite] ERROR inserting into DB '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            raise
        finally:
            if conn:
                conn.close()


    def _sync_read_operation(self, database_file: str, query_conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        if not os.path.exists(db_path_abs):
            self.log.warning(f"[SQLite] Database file not found: {db_path_abs}. Returning empty list.")
            return []
        conn = None
        results = []
        self.log.debug(f"{db_path_abs}: READ with conditions: {query_conditions}")
        try:
            conn = sqlite3.connect(db_path_abs)
            conn.row_factory = sqlite3.Row
            self._sync_create_table_if_not_exists(conn)
            where_clauses = []
            where_values = []
            for col_name, value in query_conditions.items():
                if col_name in self.file_table_columns:
                    where_clauses.append(f"{col_name} = ?")
                    # Convert query values for boolean columns to int
                    if col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                        where_values.append(1 if value else 0)
                    # Convert query values for BLOB columns to bytes
                    elif col_name == "encryption_key_auto" and isinstance(value, str):
                        where_values.append(value.encode('utf-8'))  # Assume it was originally stored as bytes
                    else:
                        where_values.append(str(value))  # Convert all other query values to string
                else:
                    self.log.warning(f"[SQLite] WARNING: Query condition for unknown column '{col_name}' ignored.")
            sql_query = "SELECT * FROM file_metadata_table"
            if where_clauses:
                sql_query += " WHERE " + " AND ".join(where_clauses)
            self.log.debug(f"SQL Query: {sql_query}, Values: {where_values}")
            cursor = conn.execute(sql_query, tuple(where_values))
            rows = cursor.fetchall()
            for row in rows:
                entry_data = {}
                for col_name in self.file_table_columns:
                    value = row[col_name]
                    entry_data[col_name] = value  # Get raw value from DB

                # Convert values back to Python types
                converted_entry = {}
                for k, v in entry_data.items():
                    if k in ["part_number", "total_parts", "message_id", "channel_id"]:
                        converted_entry[k] = int(v) if v is not None else 0
                    elif k in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                        converted_entry[k] = bool(v) if v is not None else False  # Convert int (1/0) to bool
                    elif k == "encryption_key_auto":
                        converted_entry[k] = v if v is not None else b''  # Keep as bytes
                    elif k == "password_seed_hash":
                        converted_entry[k] = v if v is not None else ""
                    elif k == "encryption_mode":
                        converted_entry[k] = v if v is not None else "off"
                    elif k == "original_root_name":
                        converted_entry[k] = v if v is not None else ""
                    elif k == "original_base_filename":
                        converted_entry[k] = v if v is not None else ""
                    elif k == "version":  # NEW: Handle version column
                        converted_entry[k] = v if v is not None else ""
                    else:
                        converted_entry[k] = v
                results.append(converted_entry)
            self.log.info(
                f"[SQLite] Successfully read {len(results)} entries from '{database_file}' matching conditions.")
            return results
        except Exception as e:
            self.log.error(f"[SQLite] ERROR reading from DB '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            return []
        finally:
            if conn:
                conn.close()


    def _sync_delete_operation(self, database_file: str, deletion_targets: List[Dict[str, Any]]) -> int:
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        if not os.path.exists(db_path_abs):
            self.log.warning(f"[SQLite] Database file not found: {db_path_abs}. No rows to delete.")
            return 0
        conn = None
        deleted_count = 0
        try:
            print(db_path_abs + ": DELETE")
            conn = sqlite3.connect(db_path_abs)
            conn.row_factory = sqlite3.Row
            self._sync_create_table_if_not_exists(conn)
            delete_conditions = []
            for target_cond in deletion_targets:
                root_upload_name = target_cond.get("root_upload_name")
                relative_path_in_archive = (target_cond.get("relative_path_in_archive") or "").replace(os.path.sep,
                                                                                                       '/').strip('/')
                base_filename = target_cond.get("base_filename")
                version = target_cond.get("version")  # NEW: Get version for deletion

                if version is not None:
                    delete_conditions.append((root_upload_name, relative_path_in_archive, base_filename, version))
                else:
                    delete_conditions.append((root_upload_name, relative_path_in_archive, base_filename))

            if not delete_conditions:
                self.log.warning(f"[SQLite] No valid deletion targets provided for '{database_file}'.")
                return 0

            delete_sql_parts = []
            delete_values = []
            for target in delete_conditions:
                if len(target) == 4:  # If version is included
                    delete_sql_parts.append(
                        "(root_upload_name = ? AND relative_path_in_archive = ? AND base_filename = ? AND version = ?)"
                    )
                else:  # If version is not included (e.g., deleting all versions of an item)
                    delete_sql_parts.append(
                        "(root_upload_name = ? AND relative_path_in_archive = ? AND base_filename = ?)"
                    )
                delete_values.extend(target)

            delete_sql = f"""
            DELETE FROM file_metadata_table
            WHERE {" OR ".join(delete_sql_parts)};
            """
            cursor = conn.execute(delete_sql, tuple(delete_values))
            deleted_count = cursor.rowcount
            conn.commit()
            self.log.info(f"[SQLite] Deleted {deleted_count} entries from '{database_file}'.")
            return deleted_count
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"[SQLite] ERROR deleting from DB '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            raise
        finally:
            if conn:
                conn.close()


    def _sync_vacuum_operation(self, database_file: str):
        """
        Synchronously performs a VACUUM operation on the SQLite database.
        This compacts the database file and reclaims unused space.
        """
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        if not os.path.exists(db_path_abs):
            self.log.warning(f"[SQLite] Database file not found: {db_path_abs}. Cannot VACUUM.")
            return
        conn = None
        try:
            conn = sqlite3.connect(db_path_abs)
            self.log.info(f"[SQLite] Attempting VACUUM on '{database_file}'...")
            conn.execute("VACUUM;")
            conn.commit()
            self.log.info(f"[SQLite] Successfully VACUUMed database: '{database_file}'.")
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"[SQLite] ERROR during VACUUM on '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            raise
        finally:
            if conn:
                conn.close()
    async def _db_insert_sync(self, database_file: str, entry: Dict[str, Any]):
        await asyncio.to_thread(self._sync_insert_operation, database_file, entry)

    async def _db_read_sync(self, database_file: str, query_conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._sync_read_operation, database_file, query_conditions)

    async def _db_delete_sync(self, database_file: str, deletion_targets: List[Dict[str, Any]]) -> int:
        return await asyncio.to_thread(self._sync_delete_operation, database_file, deletion_targets)

    async def _db_vacuum_sync(self, database_file: str):
        await asyncio.to_thread(self._sync_vacuum_operation, database_file)
    def _normalize_db_file_path(self, DB_FILE: str) -> str:
        """Ensure DB_FILE ends with .db and return absolute normalized path."""
        if not DB_FILE.lower().endswith('.db'):
            DB_FILE += '.db'
        return os.path.abspath(os.path.normpath(DB_FILE))