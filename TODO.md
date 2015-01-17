-   Throw a separate `ProtocolException` for errors like failure to parse a
    response. Get rid of calls to `fail` in favor of this exception.
-   Return an `InvalidMessageError` if the checksum for a parsed `Message`
    fails to match.
-   Tests.
