import datetime
import os
import sys
from collections import namedtuple
import threading
import time
import xmlrpc.client

import argparse
import magic
from termcolor import colored
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import DirMovedEvent
from watchdog.events import FileMovedEvent



def root_to_baser(base_path):
    def inner(full_path):
        return full_path.replace(base_path + '/', '')
    return inner



def get_mime_type(file_path):
    plain_text_mime_types = {
        'xml': 'xml',
        'xql': 'xquery'
    }

    try:
        file_extension = file_path.split('.')[-1]   
        mime = f'application/{plain_text_mime_types[file_extension]}'
        return mime
    except:
        return magic.from_file(file_path, mime=True)



Document = namedtuple('Document', ['path'])
Directory = namedtuple('Directory', ['path'])



class ExistSync():
    def __init__(self, 
                 username=None, 
                 password=None, 
                 address=None, 
                 port=8080,
                 app_base_folder='',
                 local_base_folder=''):

        assert app_base_folder, 'Must provide a base folder for the application'

        rpc_string = f'http://{username}:{password}'
        rpc_string += f'@{address}:{port}/exist/xmlrpc'

        self.rpc = xmlrpc.client.ServerProxy(
            rpc_string, 
            encoding='UTF-8', 
            verbose=False,
            use_builtin_types=True
        )
        self.local_path = local_base_folder
        self.app_path = f'/db/apps/{app_base_folder}/'

        # Create the base collection if not there already
        print('Checking eXist collection exists...')
        self.create_dir(self.app_path)
        


    def copy_file(self, full_file_path):
        root_to_base = root_to_baser(self.local_path)

        _local_file_modified_timestamp = os.path.getmtime(full_file_path)
        local_file_modified = datetime.datetime.utcfromtimestamp(_local_file_modified_timestamp)
        
        relative_file_path = root_to_base(full_file_path)
        
        exist_file_modified = self.get_exist_file_modified_datetime(relative_file_path)


        if exist_file_modified and exist_file_modified > local_file_modified:
            print(colored(f' {relative_file_path}'.ljust(10), 'yellow',), 
                'newer in eXist version:', colored('SKIPPING', 'blue'))
            return
        
        if not exist_file_modified:
            print(colored(f' {relative_file_path}'.ljust(10), 'yellow'), 'not in eXist:', colored('COPYING', 'yellow'), end='')
        else:
            print(colored(f' {relative_file_path}'.ljust(10), 'yellow'), 'newer in local version:', colored('SYNCING', 'yellow'), end='')

        try:
            
            with open(full_file_path, 'rb') as f:
                file_contents = f.read() 
            
            f_id = self.rpc.upload(file_contents, len(file_contents))
            self.rpc.parseLocal(
                f_id,                                              # file id from chunk upload
                f'{self.app_path}{relative_file_path}',            # file name to assign
                1,                                                 # overwrite existing file
                get_mime_type(full_file_path)                               # mime type
            )
            self.rpc.setPermissions(f'{self.app_path}{relative_file_path}', 493)
            print(':', colored('SUCCESS', 'green'))
        except:
            print(':', colored('FAIL', 'red'))
        

    def dir_exists(self, dir_path):
        try:
            resp = self.rpc.describeCollection(f'{self.app_path}{dir_path}')
            return True
        except xmlrpc.client.Fault:
            return False

    def create_dir(self, full_dir_path):
        root_to_base = root_to_baser(self.local_path)
        dir_path = root_to_base(full_dir_path)

        if self.dir_exists(dir_path):
            print(' DIR', colored(f'/{dir_path}', 'yellow'), 'already in eXist:', colored('SKIPPING', 'blue'))
        else:
            print(' DIR', colored(f'/{dir_path}', 'yellow'), 'not in eXist:', colored('CREATING', 'yellow'), end='')
            try:
                self.rpc.createCollection(f'{self.app_path}{dir_path}')
                print(':', colored('SUCCESS', 'green'))
            except:
                print(':', colored('FAIL', 'red'))


    def sync_up(self):
        self.clean_exist()
        self.copy_dir(self.local_path)
        

    def copy_dir(self, full_dir_path):
        for path, dirs, files in os.walk(full_dir_path):
            for d in dirs:
                self.create_dir(f'{path}/{d}')
            for f in files:
                self.copy_file(f'{path}/{f}')


    def get_exist_file_modified_datetime(self, file_path):
        resp = self.rpc.describeResource(f'{self.app_path}/{file_path}')
        if not resp:
            return None
        return resp['modified']


    def remove_dir(self, full_dir_path):
        root_to_base = root_to_baser(self.local_path)
        dir_path = root_to_base(full_dir_path)

        try:
            self.rpc.describeCollection(f'{self.app_path}{dir_path}')
        except xmlrpc.client.Fault:
            return

        print(' DIR', colored(f'/{dir_path}', 'yellow'), 'moved/deleted:', colored('DELETING OLD', 'yellow'), end='')
        try:
            self.rpc.removeCollection(f'{self.app_path}{dir_path}')
            print(':', colored('SUCCESS', 'green'))
        except:
            print(':', colored('FAIL', 'red'))


    def remove_file(self, full_file_path):

        root_to_base = root_to_baser(self.local_path)
        file_path = root_to_base(full_file_path)

        if not self.rpc.describeResource(f'{self.app_path}{file_path}'):
            return

        print(colored(f' {file_path}', 'yellow'), 'moved/deleted:', colored('DELETING OLD', 'yellow'), end='')
        try:
            self.rpc.remove(f'{self.app_path}{file_path}')
            print(':', colored('SUCCESS', 'green'))
        except:
            print(':', colored('FAIL', 'red'))


    def clean_exist(self):
        for elem in self.walk_exist_collection():
            if type(elem) is Document:
                if not os.path.isfile(f'{self.local_path}/{elem.path}'):
                    self.remove_file(f'{self.local_path}/{elem.path}')
            elif type(elem) is Directory:
                if not os.path.isdir(f'{self.local_path}/{elem.path}'):
                    self.remove_dir(f'{self.local_path}/{elem.path}')


    def walk_exist_collection(self):

        def recursive_walk(coll=self.app_path[:-1]):
            
            coll_desc = self.rpc.getCollectionDesc(coll)
            
            for document in coll_desc['documents']:
                yield Document(f'{coll}/{document["name"]}'.replace(self.app_path, ''))
            for collection in coll_desc['collections']:
                if collection != 'db':
                    yield Directory(f'{coll}/{collection}'.replace(self.app_path, ''))
                    yield from recursive_walk(f'{coll}/{collection}')

        yield from recursive_walk()






class FileWatcher(FileSystemEventHandler):
    def __init__(self, exist_sync, *args, **kwargs):
        self.e = exist_sync
        super().__init__(*args, **kwargs)

        self.move_cache = []


    def do_move_cache(self):
        time.sleep(0.1)
        
        tasks = sorted(self.move_cache, key=lambda t: str(type(t)))
        
        for task in tasks:
            if type(task) is DirMovedEvent:
                e.remove_dir(task.src_path)
                e.create_dir(task.dest_path)

            if type(task) is FileMovedEvent:
                e.remove_file(task.src_path)
                e.copy_file(task.dest_path)



        self.move_cache = []
        


  
    def on_any_event(self, event):
        
        if event.event_type == 'moved':
            
            # This move_cache is to collect all changes in event of a dir being moved
            # and to make sure the dir events are passed to eXist before the file events
            if not self.move_cache:
                t = threading.Thread(target=self.do_move_cache)
                t.start()
         
            self.move_cache.append(event)
            
        elif event.event_type == 'modified' and not event.is_directory:
            e.copy_file(event.src_path)


        elif event.event_type == 'created':
            if event.is_directory:
                e.create_dir(event.src_path)
            else:
                e.copy_file(event.src_path)

        elif event.event_type == 'deleted':
            if event.is_directory:
                e.remove_dir(event.src_path)
            else:
                e.remove_file(event.src_path)





        



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Sync directories with an eXist-DB instance')
    parser.add_argument('action', help='Action to carry out: [sync-up]')
    parser.add_argument('watch', nargs='?', help='Action to carry out: [watch]')
    parser.add_argument('-d', '--dir', help='Local directory to sync to eXist-DB')
    parser.add_argument('-e', '--exist', help='Address of eXist-DB instance, e.g. localhost, 196.168.0.1')
    parser.add_argument('-p', '--port', help='Port of the eXist instance, e.g. 8080 ')
    parser.add_argument('-c', '--collection', help='Name of base collection in eXist')
    parser.add_argument('-u', '--username', help='username for eXist instance')
    parser.add_argument('-a', '--password', help='password for eXist instance')
    args = parser.parse_args()

  
    print(f'\n{colored("eXist-DB Synchroniser", "green")}')
    print(f'{colored("=====================", "green")}\n')

    e = ExistSync(username=args.username, 
                  password=args.password, 
                  address=args.exist, 
                  port=args.port, 
                  app_base_folder=args.collection, 
                  local_base_folder=args.dir)

    if args.action == 'sync-up':
        print(f'\n\n{colored("Syncing initial state", "green")}')
        print(f'{colored("---------------------", "green")}\n')
        e.sync_up()

        if args.watch:
            print(f'\n\n{colored("Watching folder for changes...", "green")}')
            print(f'{colored("------------------------------", "green")}\n')
            event_handler = FileWatcher(exist_sync=e)
            observer = Observer()
            observer.schedule(event_handler, path='tests/TEST_SYNC_FOLDER/', recursive=True)
            observer.start()

            try:
                while True:
                    pass
            except KeyboardInterrupt:
                observer.stop()
            observer.join()



