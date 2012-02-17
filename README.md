SQLDump
=======

                            (
                             )
                        (   (
                         )   b
                        (    88_
                          ___888b__
                        _d888888888b   (
               (    ___d888888888888_   )
                )  d88888888888888888b (
               (  d8888888888888888888__
               ___8888888888888888888888b
              d88888888888888888888888888b
              888888888888888888888888888P
              Y8888888888888888888888888P

    Usage: sqldump [OPTIONS] SERVER DATABASE [TABLES]

    Options:
      -i, --use-integrated-security
                                 use Integrated Security to connect to server
                                   (default)
      -s, --use-sql-server-authentication
                                 use SQL Server authentication to connect to
                                   server
      -u, --username=VALUE       username for SQL Server authentication
      -p, --password=VALUE       password for SQL Server authentication
      -l, --limit=VALUE          limit number of records per table
      -t, --use-transaction      wrap all insert statements in a transaction
      -d, --identity-insert      include statement to enable identity insert and
                                   include identity column in output
      -e, --exclude              supplied tables are excluded, rather than
                                   included
      -?, --help                 display this help and exit
          --version              output version information then exit