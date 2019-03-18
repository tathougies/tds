# tds: Low-level Haskell TDS interface

This is a low-level client library for databases that support the TDS wire protocol (SQL Server and Sybase for example).

This library does not provide high-level primitives to parse row results. Rather, it works on converting the underlying token streams into something off of which higher-level parsers can be 
efficiently developed.

This library is used in `beam-mssql` to provide wire-protocol support.
