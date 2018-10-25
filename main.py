import datetime
import os
import sys
import xmlrpc.client

import argparse
import magic
from termcolor import colored



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
            print(colored(f' {relative_file_path}'.ljust(10), 'yellow'), 'newer in local version:', colored('COPYING', 'yellow'), end='')
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
        
        
        for path, dirs, files in os.walk(self.local_path):
            for d in dirs:
                self.create_dir(f'{path}/{d}')
            for f in files:
                self.copy_file(f'{path}/{f}')



    def get_exist_file_modified_datetime(self, file_path):
        resp = self.rpc.describeResource(f'{self.app_path}/{file_path}')
        if not resp:
            return None
        return resp['modified']


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


def sync(args):
    if args.action == 'sync-up':
        pass


if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser(description='Sync directories with an eXist-DB instance')
    parser.add_argument('action', help='Action to carry out: [sync-up]')
    parser.add_argument('-d', '--dir', help='Local directory to sync to eXist-DB')
    parser.add_argument('-e', '--exist', help='Address and port of eXist-DB instance: localhost:8080')
    args = parser.parse_args()
    sync(args)
    '''
    print('\nSyncing with eXist')
    print('------------------\n')
    e = ExistSync(username='admin', password='', address='localhost', port=8080, app_base_folder='test', local_base_folder='tests/TEST_SYNC_FOLDER')
    e.sync_up()

    #walk_folder('tests/TEST_SYNC_FOLDER', e=e)
    
    #print('exist', e.get_exist_file_modified_datetime('test1.xql'))
    '''
    e.copy_file('tests//test1.xql')
    e.copy_file('tests/TEST_SYNC_FOLDER/main.xql')
    e.copy_file('tests/TEST_SYNC_FOLDER/image.jpg')
    e.copy_file('tests/TEST_SYNC_FOLDER/main2.xql')
    e.create_dir('tests/TEST_SYNC_FOLDER/data')
    e.copy_file('tests/TEST_SYNC_FOLDER/data/test.xml')
    
    '''
    