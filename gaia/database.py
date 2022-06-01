import asyncio
import hashlib
import inspect
import json
import os
import re
import shlex
import shutil
import subprocess
from argparse import ArgumentParser
from datetime import datetime, timezone
from getpass import getpass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy as sql
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, delete, inspection, text, update, util
from sqlalchemy.cimmutabledict import immutabledict
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import ClauseElement, Executable
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.type_api import TypeEngine

from gaia.environment import DOCKER
from gaia.inputs import get_input, get_yes_no_input

# True if the current platform has the "pg_dump" command available.
IS_PG_DUMP_AVAILABLE = shutil.which("pg_dump") is not None
# True if the current platform has the "psql" command available.
IS_PSQL_AVAILABLE = shutil.which("psql") is not None

# Saved configuration for the last external database used.
SAVED_EXTERNAL_CONFIG_PATH = os.path.join(
    os.path.expanduser("~"),
    ".db-manager-saved-external-config",
)

T = TypeVar("T")
Key = Union[str, int]

if TYPE_CHECKING:
    KeyColumn = Column[TypeEngine[Key]]
else:
    KeyColumn = Column

WhereExpression = Union[ClauseElement, str]
OrderByExpression = Union[ColumnElement, str]


class SavedDatabaseConfig(BaseModel):
    host: Optional[str]
    port: Optional[int]
    database: Optional[str]
    user: Optional[str]


class DatabaseManager:
    """
    Database manager that abstracts over SQLAlchemy's engine types and provides a CLI interface.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        echo: bool = False,
        safe_mode: bool = False,
        const_table_names: Optional[Iterable[str]] = None,
        const_table_name_patterns: Optional[Iterable[str]] = None,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create a new database manager using the provided settings.

        :param host: The host of the database server to connect to. If the hostname is has a ":"
            character, it is assumed to be in the format "external-host:docker-host", where
            "external-host" is the host to connect to outside of Docker and "docker-host" is the
            host to connect to otherwise. IE, DB_HOST="127.0.0.1:db".
        :param port: The port the database will bind to.
        :param database: The name of the database to connect to.
        :param user: The database user to authenticate as.
        :param password: The database user's password to authenticate with.
        :param echo: If set to `True`, all database queries will be logged.
        :param safe_mode: If set to `True`, data management actions which destroy data are blocked.
        :param const_table_names: Names of database tables to consider constant. The `clear` command
            will not that have any of the provided names. Run the `tables --const` command to see
            which tables match.
        :param const_table_name_patterns: Regexes of database tables to consider constant. The
            `clear` command will not touch tables with names that match one or more of the provided
            regular expressions. Run the `tables --const` command to see which tables match.
        """

        if ":" in host:
            external, docker = host.split(":")
            self._host = docker if DOCKER else external
        else:
            self._host = host

        self._port = port
        self._database = database
        self._user = user
        self._password = password

        engine_config = {
            "echo": echo,
            "pool_pre_ping": True,  # Check to see if a connection has closed before use.
            "pool_recycle": 60 * 5,  # Drop unused connections after 5 minutes.
            **(engine_config or {}),
        }

        self._engine: AsyncEngine = create_async_engine(self.url, **engine_config)

        self._safe_mode_is_enabled = safe_mode
        self._cli_mode_is_enabled = False

        self._const_table_names: Sequence[str] = list(const_table_names or [])
        self._const_table_name_patterns: Sequence[str] = list(const_table_name_patterns or [])

        self._argument_parser = ArgumentParser(
            description="Common actions for management of application data.",
        )
        self._argument_parser_subparsers = self._argument_parser.add_subparsers()

        self._add_command(self.ping, help="verify the database is accessible")

        dump_command = self._add_command(self.dump, help="export all data to a dump file")
        dump_command.add_argument("sql_dump_destination", help="file or directory to dump data to")
        dump_command.add_argument(
            "--external",
            action="store_true",
            help="dump data from a database outside the project",
        )
        dump_command.add_argument(
            "--const",
            action="store_true",
            help="dump only const tables",
        )
        dump_command.add_argument(
            "--non-const",
            action="store_false",
            help="dump only non-const tables",
            dest="const",
        )

        load_command = self._add_command(self.load, help="import all data from a dump file")
        load_command.add_argument("sql_dump_file", help="file to load data from")

        clear_command = self._add_command(self.clear, help="truncate database tables")
        clear_command.add_argument(
            "--include-const",
            action="store_true",
            help="also truncate const tables",
            dest="include_const",
        )

        tables_command = self._add_command(self.tables, help="list tables")
        tables_command.add_argument(
            "--const",
            action="store_true",
            help="show only const tables",
            default=None,
        )
        tables_command.add_argument(
            "--non-const",
            action="store_false",
            help="show only non-const tables",
            dest="const",
            default=None,
        )
        tables_command.add_argument(
            "--empty",
            action="store_true",
            help="show only empty tables",
            default=None,
        )
        tables_command.add_argument(
            "--non-empty",
            action="store_false",
            help="show only non-empty tables",
            dest="empty",
            default=None,
        )

        self._add_command(self.schema, help="show information about the current database schema")

    @property
    def url(self) -> str:
        """
        Get URL of the database used for the asyncronous engine. This includes username and password
        authentication.
        """
        return f"postgresql+asyncpg://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"

    @property
    def engine(self) -> AsyncEngine:
        """
        Access the underlying database async engine.
        """
        return self._engine

    def main(self) -> None:
        """
        Run the command line interface.
        """
        exit(asyncio.run(self._main()))

    async def _main(self) -> int:
        """
        Run the command line interface asyncronously.
        """
        self._cli_mode_is_enabled = True

        arguments = self._argument_parser.parse_args()
        command = getattr(arguments, "command", None)

        if command is None:
            self._argument_parser.print_help()
            result = None
        else:
            if command.__name__ != self.ping.__name__:
                if not await self.ping(show=False):
                    print("Unable to connect to the database.")
                    return 1

            delattr(arguments, "command")
            result = command(**vars(arguments))
            if inspect.iscoroutine(result):
                result = await result

        await self.dispose()

        if isinstance(result, bool):
            return 0 if result else 1
        elif isinstance(result, int):
            return result
        else:
            return 0

    async def dispose(self) -> None:
        """
        Discard all active database connections.
        """
        await self._engine.dispose()

    async def ping(self, show: bool = True) -> bool:
        """
        Check if the database can be accessed.
        """
        try:
            async with self.engine.connect():
                if self._cli_mode_is_enabled and show:
                    print("Able to connect to the database.")
                return True
        except:
            if self._cli_mode_is_enabled and show:
                print("Unable to connect to the database.")
            return False

    async def dump(
        self,
        sql_dump_destination: str,
        *,
        external: bool = False,
        const: Optional[bool] = None,
    ) -> str:
        """
        Save all data in the project database (or another external database) as an SQL dump file.

        :param sql_dump_destination: The path to dump data to. If the base name of the destination
            path includes a file extension such as `.sql`, data will be dumped directly to that
            path. Otherwise, the path is treated as a directory and data will be written to a
            timestamped file inside of it.
        :param external: If `True`, dump data from a different database outside the project.
        :param const: If `None`, dump data from both const and non-const tables. If `True`, only
            dump const tables. If `False', only dump non-const tables.
        """
        if not IS_PG_DUMP_AVAILABLE:
            raise AssertionError(
                'The "pg_dump" command must be in the system path to execute "dump".'
            )

        host = self._host
        port = self._port
        database = self._database
        user = self._user
        password = self._password

        if external:
            config = SavedDatabaseConfig()
            saved = self._read_saved_config(SAVED_EXTERNAL_CONFIG_PATH)

            if saved.dict(exclude_unset=True):
                print(f"Previous external database config: {saved.json(indent=2)}")
                if get_yes_no_input("Use previous external database config?", True):
                    config = saved

            host = config.host or get_input(
                str,
                "Database Host",
                saved.host or "127.0.0.1",
            )

            port = config.port or get_input(int, "Database Port", saved.port)
            database = config.database or get_input(str, "Database Name", saved.database)
            user = config.user or get_input(str, "Database User", saved.user)
            password = getpass("Database Password: ")

            self._write_saved_config(
                SAVED_EXTERNAL_CONFIG_PATH,
                SavedDatabaseConfig(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                ),
            )

        if const is None:
            tables = []
        else:
            tables = await self.tables(const=const, show=False)

        if "." in os.path.basename(sql_dump_destination):
            sql_dump_file = os.path.realpath(os.path.expanduser(sql_dump_destination))
        else:
            timestamp = datetime.now(timezone.utc).isoformat("T")
            sql_dump_file = os.path.realpath(
                os.path.expanduser(os.path.join(sql_dump_destination, f"{timestamp}.sql"))
            )

        os.makedirs(os.path.dirname(sql_dump_file), exist_ok=True)

        command = shlex.join(
            [
                "pg_dump",
                database,
                "--schema=public",
                "--host",
                host,
                "--port",
                str(port),
                "--username",
                user,
                "--file",
                sql_dump_file,
                "--data-only",
                "--disable-triggers",
                *(f"--table={table}" for table in tables),
            ]
        )

        self._run_command(command, password)

        return sql_dump_file

    def load(self, sql_dump_file: str) -> None:
        """
        Load data from an SQL dump file into the project database.

        :params sql_dump_file: The SQL dump file to load.
        """
        if not IS_PSQL_AVAILABLE:
            raise AssertionError('The "psql" command must be in the system path to execute "load".')

        host = self._host
        port = self._port
        database = self._database
        user = self._user

        command = (
            shlex.join(
                [
                    "psql",
                    database,
                ]
            )
            + " < "
            + shlex.join(
                [
                    sql_dump_file,
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--username",
                    user,
                    "--single-transaction",
                    "--variable",
                    "ON_ERROR_STOP=1",
                ]
            )
        )

        self._run_command(command)

    async def clear(
        self,
        *,
        confirm: Optional[bool] = None,
        include_const: bool = False,
    ) -> Optional[int]:
        """
        Truncate database tables. Only non-const tables are cleared by default. Cannot be run in
        safe mode.

        :param confirm: If `True` (or `None` in CLI mode) a CLI confirmation prompt will appear
            to confirm the command before executing.
        :param include_const: If `True`, clear both const and non-const tables.
        """
        if self._safe_mode_is_enabled:
            raise PermissionError("Clearing the database is not allowed in safe mode.")

        confirm = self._cli_mode_is_enabled if confirm is None else confirm

        if confirm:
            print("This action will truncate the following tables:")

        tables = await self.tables(const=None if include_const else False, show=confirm)

        if confirm:
            if not get_yes_no_input("Continue?", False):
                print("Action cancelled. No data was lost.")
                return 1

        async with self._engine.begin() as connection:
            for table in tables:
                await connection.execute(text(f"TRUNCATE TABLE {self._escape(table)} CASCADE"))
            await connection.execute(text(_SQL_FIX_SEQUENCES))

        return None

    async def tables(
        self,
        *,
        const: Optional[bool] = None,
        empty: Optional[bool] = None,
        show: bool = True,
    ) -> List[str]:
        """
        List tables in the project database.

        :params const: If set to a `True`, list all const tables. List only non-const if `False`.
            List both if `None`.
        :params empty: If set to a `True`, list only empty tables. List only non-empty if `False`.
            List both if `None`.
        :params show: If set to a `True`, tables will be printed to the stdout.
        """

        async with self._engine.connect() as connection:
            rows = await connection.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_type='BASE TABLE'
                    """
                )
            )

            tables = sorted(row["table_name"] for row in rows)

            if const is not None:
                tables = [table for table in tables if self._is_const_table(table) == const]
            if empty is not None:
                tables = [table for table in tables if (await self._is_empty_table(table)) == empty]

            if show and self._cli_mode_is_enabled:
                for table in tables:
                    count = (
                        await connection.execute(
                            text(
                                f"""
                                SELECT COUNT(*) FROM {self._escape(table)}
                                """,
                            )
                        )
                    ).scalar()

                    print(f"* {table} ({count})")

        return tables

    async def schema(self) -> Dict[str, Any]:
        """
        Get information about the current database schema.
        """
        selects = {
            "columns": "SELECT * FROM information_schema.columns WHERE table_schema = 'public'",
            "views": "SELECT * FROM information_schema.views WHERE table_schema = 'public'",
            "keys": "SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'public'",
            "indexes": "SELECT * FROM pg_catalog.pg_indexes WHERE schemaname = 'public'",
            "constraints": "SELECT * FROM information_schema.check_constraints WHERE constraint_schema = 'public'",
        }

        ignore = [
            "ordinal_position",
            "position_in_unique_constraint",
        ]

        schema: Dict[str, Any] = {}

        async with self._engine.connect() as connection:
            async with connection.begin():
                for name, select in selects.items():
                    rows = await connection.execute(text(select))
                    schema[name] = []
                    for row in rows:
                        data = dict(row)
                        for column in ignore:
                            data.pop(column.lower(), None)
                        schema[name].append(data)

        output = {
            "schema": schema,
            "hash": hashlib.md5(json.dumps(schema, sort_keys=True).encode("utf-8")).hexdigest(),
        }

        if self._cli_mode_is_enabled:
            print(json.dumps(output, indent=2))

        return output

    def _run_command(self, command: str, password: Optional[str] = None) -> None:
        if password is None:
            password = self._password

        env = os.environ.copy()
        env["PGPASSWORD"] = password
        subprocess.run(
            command,
            check=True,
            shell=True,
            env=env,
            capture_output=True,
        )

    def _read_saved_config(self, path: str) -> SavedDatabaseConfig:
        try:
            with open(path) as file:
                return SavedDatabaseConfig(**json.load(file))
        except:
            pass

        return SavedDatabaseConfig()

    def _write_saved_config(self, path: str, config: SavedDatabaseConfig) -> None:
        with open(path, "w") as file:
            file.write(config.json(indent=2))

    def _is_const_table(self, table: str) -> bool:
        if table in self._const_table_names:
            return True
        if any(re.match(pattern, table) for pattern in self._const_table_name_patterns):
            return True

        return False

    async def _is_empty_table(self, table: str) -> bool:
        async with self._engine.connect() as connection:
            first = (
                await connection.execute(text(f"SELECT TRUE FROM {self._escape(table)} LIMIT 1"))
            ).first()

            return first is None

    def _escape(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _add_command(
        self,
        function: Any,
        name: Optional[str] = None,
        help: Optional[str] = None,
    ) -> Any:
        command = self._argument_parser_subparsers.add_parser(
            name=name or function.__name__,
            help=help or inspect.getdoc(function),
        )
        command.set_defaults(command=function)
        return command


class Session:
    """
    Strongly typed wrapper around SQLAlchemy's `AsyncSession` type.
    """

    if TYPE_CHECKING:
        _session_maker: sessionmaker[AsyncSession]

    def __init__(self, database: DatabaseManager) -> None:
        self._database = database
        self._inner: Optional[AsyncSession] = None
        self._session_maker = sessionmaker(
            self._database.engine,
            autocommit=False,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    @property
    def database(self) -> DatabaseManager:
        """
        Access database manager this session is bound to.
        """
        return self._database

    @property
    def inner(self) -> AsyncSession:
        """
        Access the inner SQLAlchemy session.
        """
        inner = self._inner
        if inner is None:
            inner = self._session_maker()
            self._inner = inner

        return inner

    async def __aenter__(self) -> "Session":
        """
        Allow usage of "async with" to automatically close the session.
        """
        return self

    async def __aexit__(self, type: Any, value: Any, traceback: Any) -> None:
        """
        At the end of an "async with" close the session.
        """
        await self.close()

    async def get(
        self,
        entity_type: Type[T],
        key: Optional[Key],
        *,
        options: Optional[Iterable[Any]] = None,
    ) -> Optional[T]:
        """
        Return an entity of a given type that has a matching primary key, or `None` if the entity is
        not found.

        :param entity_type: The class of entity to get.
        :param key: The primary key to use to find the entity.
        :param options: SQLAlchemy query loader options.

        Example:

        ```
        # Get a user with an ID equal to 100.
        user = await session.get(User, 100)
        ```
        """
        if key is None:
            return None

        column_type = type(self._get_primary_key_column(entity_type).type)
        if issubclass(column_type, String) and not isinstance(key, str):
            key = str(key)
        if issubclass(column_type, Integer) and not isinstance(key, int):
            try:
                key = int(key)
            except:
                return None

        return cast(
            Optional[T],
            await self.inner.get(
                entity_type,
                key,
                options=None if options is None else list(options),
                populate_existing=True,
            ),
        )

    async def find(
        self,
        entity_type: Type[T],
        where: Optional[WhereExpression] = None,
        *,
        order_by: Optional[OrderByExpression] = None,
        options: Optional[Iterable[Any]] = None,
    ) -> Optional[T]:
        """
        Return the first entity of a given type that matches a condition, or `None` if no entity is
        found.

        :param entity_type: The class of entity to find.
        :param where: The condition to match.
        :param options: SQLAlchemy query loader options.

        Example:

        ```
        # Find a user with the username "Steve".
        user = await session.find(User, User.username == "steve")
        ```
        """
        query = sql.select(entity_type)
        if where is not None:
            query = query.where(where)
        if order_by is not None:
            query = query.order_by(order_by)

        if options is not None:
            query = query.options(*options)

        return cast(Optional[T], (await self.inner.execute(query.limit(1))).scalars().first())

    async def all(
        self,
        entity_type: Type[T],
        where: Optional[WhereExpression] = None,
        *,
        order_by: Optional[OrderByExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        options: Optional[Iterable[Any]] = None,
    ) -> List[T]:
        """
        Return all entities of a given type that match a condition.

        :param entity_type: The class of entity to search for.
        :param where: The condition to match.
        :param options: SQLAlchemy query loader options.
        :param limit: The maximum number of results to return. By default, all results are returned.
        :param offset: The number of results to skip.

        Example:

        ```
        # Get all users with the first name "Steve".
        users = await session.all(User, User.first_name == 'Steve')
        # Get a maximum of 10 users with the first name "Mary", skipping the first 5.
        users = await session.all(User, User.first_name == 'Mary', limit=10, offset=5)
        ```
        """
        query = sql.select(entity_type)
        if where is not None:
            query = query.where(where)
        if order_by is not None:
            query = query.order_by(order_by)
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)

        if options is not None:
            query = query.options(*options)

        return cast(List[T], (await self.inner.execute(query)).scalars().all())

    async def refresh(self, instance: T, attributes: Optional[Iterable[str]] = None) -> None:
        """
        Equivalent to `AsyncSession.refresh` method.
        """
        await self.inner.refresh(
            instance,
            list(attributes) if attributes is not None else None,
        )

    def add(self, instance: T) -> T:
        """
        Equivalent to `AsyncSession.add`.
        """
        self.inner.add(instance)
        return instance

    def add_all(self, instances: Iterable[T]) -> None:
        """
        Equivalent to `AsyncSession.add_all`.
        """
        self.inner.add_all(list(instances))

    async def update_all(
        self,
        entity_type: Type[T],
        values: Mapping[str, Any],
        where: Optional[WhereExpression] = None,
    ) -> int:
        """
        Set the given values on all entities that match a where expression, if provided.

        :param entity_type: The type of entity to update.
        :param values: The column values to set.
        :param where: The where expression to filter by.
        :returns: The number of rows updated.
        """
        result: Any = await self.inner.execute(
            update(
                entity_type,
                values=values,
                whereclause=where,
            ),
            execution_options=immutabledict({"synchronize_session": "fetch"}),
        )

        return cast(int, result.rowcount)

    async def delete(self, instance: T) -> None:
        """
        Equivalent to `AsyncSession.delete`.
        """
        self.inner.delete(instance)

    async def delete_all(
        self,
        entity_type: Type[T],
        where: Optional[WhereExpression] = None,
    ) -> int:
        """
        Delete all entities that match a where expression, if provided.

        :param entity_type: The type of entity to delete.
        :param where: The where expression to filter by.
        :returns: The number of rows deleted.
        """
        result: Any = await self.inner.execute(
            delete(
                entity_type,
                whereclause=where,
            )
        )

        return cast(int, result.rowcount)

    async def execute(
        self,
        statement: Union[str, Executable],
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        execution_options: Dict[str, Any] = util.EMPTY_DICT,
        bind_arguments: Optional[Dict[str, Any]] = None,
        **kw: Dict[str, Any],
    ) -> Result:
        """
        Equivalent to `AsyncSession.execute`.
        """
        return await self.inner.execute(
            statement=text(statement) if isinstance(statement, str) else statement,
            params=params,
            execution_options=execution_options,
            bind_arguments=bind_arguments,
            **kw,
        )

    async def commit(self) -> None:
        """
        Equivalent to `AsyncSession.commit`.
        """
        await self.inner.commit()

    def begin(self) -> AsyncSessionTransaction:
        """
        Equivalent to `AsyncSession.begin`.
        """
        return self.inner.begin()

    async def rollback(self) -> None:
        """
        Equivalent to `AsyncSession.rollback`.
        """
        await self.inner.rollback()

    async def close(self) -> None:
        """
        Equivalent to `AsyncSession.close`.
        """
        await self.inner.close()

    @classmethod
    def _get_primary_key_column(cls, table: Type[T]) -> KeyColumn:
        return cast(KeyColumn, inspection.inspect(table).primary_key[0])

    @classmethod
    def _get_primary_key_value(cls, instance: T) -> Any:
        table = type(instance)
        column = cls._get_primary_key_column(table)
        return getattr(instance, column.name)


# Taken from https://wiki.postgresql.org/wiki/Fixing_Sequences.
_SQL_FIX_SEQUENCES = """
    DO $$
    DECLARE
    command TEXT;
    BEGIN
        FOR command IN (
            SELECT 'SELECT setval('
                        || quote_literal(quote_ident(tables.schemaname)
                        || '.'
                        || quote_ident(sequences.relname))
                        || ', coalesce(max(' || quote_ident(columns.attname) || '), 1))'
                    || ' FROM '
                    || quote_ident(tables.schemaname)
                    || '.'
                    || quote_ident(class.relname)
                    || ';'
            FROM pg_class AS sequences,
                pg_depend AS depends,
                pg_class AS class,
                pg_attribute AS columns,
                pg_tables AS tables
            WHERE sequences.relkind = 'S'
            AND sequences.oid = depends.objid
            AND depends.refobjid = class.oid
            AND depends.refobjid = columns.attrelid
            AND depends.refobjsubid = columns.attnum
            AND class.relname = tables.tablename
            ORDER BY class.relname
        )
        LOOP
            EXECUTE command;
        END LOOP;
    END $$;
    """
