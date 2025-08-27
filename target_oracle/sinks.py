"""Oracle target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import SQLSink
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
import os
import re
from typing import Any, Dict, Iterable, List, Optional, cast

import sqlalchemy
import oracledb
from sqlalchemy import Column, text
from sqlalchemy.dialects import oracle
from sqlalchemy.schema import PrimaryKeyConstraint
from singer_sdk.helpers._typing import get_datelike_property_type

class OracleConnector(SQLConnector):
    """The connector for Oracle with wallet authentication support."""

    allow_column_add: bool = True
    allow_column_rename: bool = True
    allow_column_alter: bool = True
    allow_merge_upsert: bool = True
    allow_temp_tables: bool = True

    @property
    def engine(self) -> sqlalchemy.Engine:
        """Back-compat for SDKs that don't expose .engine on SQLConnector."""
        if getattr(self, "_engine", None) is None:
            self._engine = self.create_engine()
        return self._engine

    def get_sqlalchemy_url(self) -> str:
        """Generate SQLAlchemy URL for Oracle with wallet support."""
        cfg = self.config

        # 1) Explicit URL wins
        if cfg.get("sqlalchemy_url"):
            return cfg["sqlalchemy_url"]

        # 2) Wallet helpers
        if cfg.get("tns_admin"):
            os.environ["TNS_ADMIN"] = cfg["tns_admin"]

        # 3) Proxy user (wallet)
        if cfg.get("proxy_user"):
            # If a target_schema is provided, use it for proxy authentication.
            if cfg.get("target_schema"):
                return f"oracle+oracledb://[{cfg['target_schema']}]@{cfg['proxy_user']}"
            # Otherwise, connect as the proxy user directly.
            return f"oracle+oracledb://@{cfg['proxy_user']}"

        # 4) DSN (wallet)
        if cfg.get("dsn"):
            return f"oracle+oracledb:///?dsn={cfg['dsn']}"

        # 5) Traditional (non-wallet) — build manually so tests see the password in the string
        if cfg.get("username") and cfg.get("password") and cfg.get("host") and cfg.get("port") and cfg.get("database"):
            return (
                f"oracle+oracledb://{cfg['username']}:{cfg['password']}"
                f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
            )

        raise ValueError(
            "Oracle connection configuration incomplete. Provide one of:\n"
            "1. sqlalchemy_url\n"
            "2. proxy_user (wallet)\n"
            "3. dsn (wallet)\n"
            "4. username, password, host, port, database (traditional)"
        )

    def create_engine(self) -> sqlalchemy.Engine:
        """Create SQLAlchemy engine with proper Oracle client initialization."""
        try:
            oracledb.init_oracle_client()
        except Exception:
            pass

        url = self.get_sqlalchemy_url()
        kwargs = self.get_engine_kwargs()
        cfg = self.config

        # Decide wallet mode from config, not from parsed URL
        wallet_mode = bool(cfg.get("proxy_user") or cfg.get("dsn"))

        if wallet_mode:
            def creator():
                if cfg.get("tns_admin"):
                    os.environ["TNS_ADMIN"] = cfg["tns_admin"]
                
                # The user for connection will be the proxy user with the target schema
                user_connect_string = ""
                if cfg.get("proxy_user") and cfg.get("target_schema"):
                    user_connect_string = f"[{cfg['target_schema']}]"

                dsn = cfg.get("proxy_user") or cfg.get("dsn")
                if not dsn:
                    raise ValueError("proxy_user or dsn required for wallet authentication")
                try:
                    # The user parameter needs to be passed for proxy auth with a DSN
                    return oracledb.connect(user=user_connect_string, dsn=dsn)
                except Exception as e:
                    # Raise RuntimeError so tests catch the exact type
                    raise RuntimeError("Failed to connect to Oracle") from e

            return sqlalchemy.create_engine("oracle+oracledb://", creator=creator, **kwargs)

        # Non wallet path
        return sqlalchemy.create_engine(url, **kwargs)


    def get_engine_kwargs(self) -> dict:
        """Engine kwargs hook."""
        return {
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        }

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Convert JSON Schema type to SQL type."""
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(sqlalchemy.types.TypeEngine, oracle.TIMESTAMP())
                if datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, oracle.TIMESTAMP())
                if datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, oracle.DATE())

            maxlength = jsonschema_type.get("maxLength")
            if maxlength is None or maxlength > 4000:
                # Use CLOB for very large strings or unlimited strings
                return cast(sqlalchemy.types.TypeEngine, oracle.CLOB())
            elif maxlength <= 4000:
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(maxlength))
            else:
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(4000))

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.INTEGER())
        
        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            if self.config.get("prefer_float_over_numeric", False):
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.FLOAT())
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NUMERIC(38, 10))
        
        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, oracle.VARCHAR(1))

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast(sqlalchemy.types.TypeEngine, oracle.CLOB())

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, oracle.CLOB())

        # Default fallback
        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(4000))

    def _jsonschema_type_check(self, jsonschema_type: dict, type_check: tuple[str]) -> bool:
        """Return True if the jsonschema_type supports the provided type."""
        # direct 'type'
        if "type" in jsonschema_type:
            tval = jsonschema_type["type"]
            if isinstance(tval, (list, tuple)):
                if any(t in type_check for t in tval):
                    return True
            else:
                if tval in type_check:
                    return True

        # anyOf: recurse into subschemas
        for subschema in jsonschema_type.get("anyOf", []) or []:
            if self._jsonschema_type_check(subschema, type_check):
                return True

        return False
    
    def prepare_schema(self, schema_name: str) -> None:
        """Ensure session uses the desired schema."""
        if not schema_name:
            return
        # No need to run ALTER SESSION if proxy authentication already handles the schema
        if not (self.config.get("proxy_user") and self.config.get("target_schema")):
            with self.engine.connect() as connection:
                connection.execute(
                    text(f"ALTER SESSION SET CURRENT_SCHEMA = {schema_name}")
                )

    def prepare_column(self, full_table_name: str, column_name: str, sql_type: sqlalchemy.types.TypeEngine) -> None:
        """Adapt target table to provided schema if possible."""
        if not self.column_exists(full_table_name, column_name):
            self._create_empty_column(
                full_table_name=full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )
            return

        if not self.config.get('freeze_schema'):
            self._adapt_column_type(
                full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )

    def _create_empty_column(self, full_table_name: str, column_name: str, sql_type: sqlalchemy.types.TypeEngine) -> None:
        """Create a new column."""
        if not self.allow_column_add:
            raise NotImplementedError("Adding columns is not supported.")

        if column_name.startswith("_"):
            column_name = f"x{column_name}"
            
        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(column_name, sql_type)
        )
        compiled = create_column_clause.compile(dialect=self._engine.dialect)

        try:
            with self.connection.begin():
                self.connection.execute(
                    sqlalchemy.text(f"ALTER TABLE {full_table_name} ADD {compiled}")
                )
        except Exception as e:
            raise RuntimeError(
                f"Could not create column '{column_name} {compiled}' on table '{full_table_name}'."
            ) from e

    def create_temp_table_from_table(self, from_table_name, temp_table_name):
        """Create temp table from another table."""
        try:
            with self.connection.begin():
                self.connection.execute(sqlalchemy.text(f"DROP TABLE {temp_table_name}"))
        except Exception:
            pass  # Table doesn't exist
        
        ddl = f"""
            CREATE TABLE {temp_table_name} AS (
                SELECT * FROM {from_table_name} WHERE 1=0
            )
        """
        with self.connection.begin():
            self.connection.execute(sqlalchemy.text(ddl))

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table."""
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        # Use the target_schema from config if available
        meta_schema = self.config.get("target_schema") or schema_name
        meta = sqlalchemy.MetaData(schema=meta_schema)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
            )

        for property_name, property_jsonschema in properties.items():
            columns.append(
                sqlalchemy.Column(property_name, self.to_sql_type(property_jsonschema))
            )
        
        if primary_keys:
            pk_constraint = PrimaryKeyConstraint(*primary_keys, name=f"{table_name}_PK")
            _ = sqlalchemy.Table(table_name, meta, *columns, pk_constraint)
        else:
            _ = sqlalchemy.Table(table_name, meta, *columns)

        meta.create_all(self._engine)

    def merge_sql_types(self, sql_types: list[sqlalchemy.types.TypeEngine]) -> sqlalchemy.types.TypeEngine:
        """Return a compatible SQL type for the selected type list."""
        if not sql_types:
            raise ValueError("Expected at least one member in `sql_types` argument.")

        if len(sql_types) == 1:
            return sql_types[0]

        current_type = sql_types[0]
        sql_type_len: int = getattr(sql_types[1], "length", 0)
        if sql_type_len is None:
            sql_type_len = 0

        sql_types = self._sort_types(sql_types)

        if len(sql_types) > 2:
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:]
            )

        assert len(sql_types) == 2
        
        for opt in sql_types:
            opt_len: int = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            if isinstance(generic_type, type):
                if issubclass(generic_type, (sqlalchemy.types.String, sqlalchemy.types.Unicode)):
                    current_len = getattr(current_type, "length", 0) or 0
                    if (opt_len is None) or (opt_len == 0) or (opt_len >= current_len):
                        return opt
                elif str(opt) == str(current_type):
                    return opt

        raise ValueError(f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}")

    def _adapt_column_type(self, full_table_name: str, column_name: str, sql_type: sqlalchemy.types.TypeEngine) -> None:
        """Adapt table column type to support the new JSON schema type."""
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(full_table_name, column_name)

        if str(sql_type) == str(current_type):
            return

        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type).split(" ")[0] == str(current_type).split(" ")[0]:
            return

        if not self.allow_column_alter:
            raise NotImplementedError(
                "Altering columns is not supported. "
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            )

        compiled_type = compatible_sql_type.compile(dialect=self._engine.dialect)

        try:
            with self.connection.begin():
                self.connection.execute(
                    sqlalchemy.text(
                        f"ALTER TABLE {full_table_name} MODIFY ({column_name} {compiled_type})"
                    )
                )
        except Exception as e:
            raise RuntimeError(
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            ) from e


def append_suffix_to_ident(ident: str, suffix: str) -> str:
    """Append a suffix to a quoted or unquoted Oracle identifier."""
    ident = ident.strip()
    if ident.startswith('"') and ident.endswith('"'):
        return f'"{ident[1:-1]}{suffix}"'
    return f"{ident}{suffix}"


def build_temp_table_name(full_table_name: str, suffix: str = "_temp") -> str:
    """Return a valid temp table FQN by appending a suffix to the table part only."""
    base_fqn = str(full_table_name)
    if "." in base_fqn:
        schema_part, table_part = base_fqn.split(".", 1)
        return f"{schema_part}.{append_suffix_to_ident(table_part, suffix)}"
    return append_suffix_to_ident(base_fqn, suffix)

class OracleSink(SQLSink):
    """Oracle target sink class."""

    soft_delete_column_name = "x_sdc_deleted_at"
    version_column_name = "x_sdc_table_version"
    connector_class = OracleConnector

    @property
    def schema_name(self) -> Optional[str]:
        """Return the target schema name or None to use default schema."""
        target_schema: str = self.config.get("target_schema", None)
        
        if target_schema:
            return target_schema

        # If no target schema specified, use the default schema (don't specify one)
        return None

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context."""
        conformed_records = (
            [self.conform_record(record) for record in context["records"]]
            if isinstance(context["records"], list)
            else (self.conform_record(record) for record in context["records"])
        )

        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

        if self.key_properties:
            self.logger.info(f"Preparing table {self.full_table_name}")
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
            )

            tmp_table_name = build_temp_table_name(self.full_table_name, "_temp")

            self.logger.info(f"Creating temp table {tmp_table_name}")
            self.connector.create_temp_table_from_table(
                from_table_name=self.full_table_name,
                temp_table_name=tmp_table_name
            )

            self.bulk_insert_records(
                full_table_name=tmp_table_name,
                schema=schema,
                records=conformed_records,
            )
            
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=tmp_table_name,
                to_table_name=self.full_table_name,
                schema=schema,
                join_keys=join_keys,
            )
        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=schema,
                records=conformed_records,
            )

    def merge_upsert_from_table(self, from_table_name: str, to_table_name: str, schema: dict, join_keys: List[str]) -> Optional[int]:
        """Merge upsert data from one table to another."""
        join_keys = [self.conform_name(key, "column") for key in join_keys]
        schema = self.conform_schema(schema)

        join_condition = " and ".join([f"temp.{key} = target.{key}" for key in join_keys])
        update_stmt = ", ".join([
            f"target.{key} = temp.{key}"
            for key in schema["properties"].keys()
            if key not in join_keys
        ])

        merge_sql = f"""
            MERGE INTO {to_table_name} target
            USING {from_table_name} temp
            ON ({join_condition})
            WHEN MATCHED THEN
                UPDATE SET {update_stmt}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(schema["properties"].keys())})
                VALUES ({", ".join([f"temp.{key}" for key in schema["properties"].keys()])})
        """

        with self.connection.begin():
            self.connection.execute(sqlalchemy.text(merge_sql))
            self.connection.execute(sqlalchemy.text(f"DROP TABLE {from_table_name}"))

    def bulk_insert_records(self, full_table_name: str, schema: dict, records: Iterable[Dict[str, Any]]) -> Optional[int]:
        """Bulk insert records to an existing destination table."""
        insert_sql = self.generate_insert_statement(full_table_name, schema)
        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        self.logger.info("Inserting with SQL: %s", insert_sql)
        columns = self.column_representation(schema)

        insert_records = []
        for record in records:
            insert_record = {}
            conformed_record = self.conform_record(record)
            for column in columns:
                insert_record[column.name] = conformed_record.get(column.name)
            insert_records.append(insert_record)

        if insert_records:
            with self.connection.begin():
                self.connection.execute(insert_sql, insert_records)

        if isinstance(records, list):
            return len(records)

        return None

    def column_representation(self, schema: dict) -> List[Column]:
        """Return a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(property_name, self.connector.to_sql_type(property_jsonschema))
            )
        return columns

    def snakecase(self, name):
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    def move_leading_underscores(self, text):
        match = re.match(r'^(_*)(.*)', text)
        if match:
            result = match.group(2) + match.group(1)
            return result
        return text

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conform a stream property name to one suitable for the target system."""
        name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
        name = self.move_leading_underscores(name)
        name = self.snakecase(name)
        if name and name[0].isdigit():
            name = f"n{name}"

        return name