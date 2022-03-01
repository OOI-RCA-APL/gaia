# Gaia

## About

Gaia is a set of utilities and libraries for web servers at APL.

## Modules

| Module      | Description                                                                         |
| ----------- | ----------------------------------------------------------------------------------- |
| auth        | Hash and verify passwords and handle user authentication using `passlib` and `jose` |
| database    | Query and manage PostgreSQL databases in a type-safe way using `sqlalchemy`.        |
| emails      | Send emails asyncronously using `aiosmtplib`.                                       |
| environment | Get information about the host machine, operating system, core count, etc.          |
| http        | Import HTTP related types for `fastapi` and `pydantic`.                             |
| inputs      | Get strongly-typed user input from the command line.                                |
| logs        | Setup and log messages easily when running on `uvicorn`.                            |
| routing     | Make `fastapi` routers a bit simpler.                                               |
| settings    | Get environment variables and `.env` files with helpful error messages.             |
