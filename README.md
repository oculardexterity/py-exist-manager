# PyExist

Version 0.1: 

## A Python command-line tool for pushing a folder of stuff to eXist-DB

So that you can develop locally, and then push to eXist-DB.

Point it at a directory and be like, *sync me*!

Also watches a folder for changes (using Watchdog)

- Currently supports:
    - Modifying files
    - Creating files
    - Creation sub-collections
    - Renaming sub-collections 
    - Deleting files/sub-collections


- It probably will need to invoke re-indexing and all that kind of thing    


Currently works like:
```
$ python main.py sync-up watch --config config.toml development
```

- Can load config from a toml file
- Or supply everything as a command line arg, which is tedious:

```
$ python main.py sync-up watch --dir tests/TEST_SYNC_FOLDER --exist localhost --port 8080 --username admin --password password --collection test

```


(Also don't see why you couldn't import the classes and use them in your application, if that was your gig.)