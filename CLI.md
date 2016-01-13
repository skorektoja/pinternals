If you look into diffo\_0.1.0.zip, there are two scripts: diffo.bat and diffo\_sample.bat.

First one is for java-related settings and executable command file.

Diffo\_sample.bat shows how you can call application:

  * set PI-specific options (SID, host, username, password)
  * list the actions (commands), which are:
    1. start -- open local database and start new session
    1. refresh -- ask your PI through SimpleQuery or another interface
    1. finish -- close session and database

(TODO: other options)