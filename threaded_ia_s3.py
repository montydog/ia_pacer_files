"""
threaded_ia_s3.py
"""

import os, sys, threading
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, Executor
import threading
import re
import csv
from collections import namedtuple
from path import Path
from internetarchive import download, get_item, search_items, get_files


def create_paths(file):
    Paths = namedtuple('Paths', ['court', 'ident', 'new_dir', 'file_path', 'file_url'])
    base_url = 'http://s3.us.archive.org'
    base_path = '/Users/admin/Documents/Bulk Legal Data/ia_s3_files2'
    #regex to parse a case identifier and extract just the court name
    court = re.search(r'gov\.uscourts\.[a-z]+', file).group()
    ident = re.search(r'gov\.uscourts\.[a-z]+\.[0-9]+', file).group()
    
    new_dir = Path.joinpath(base_path, court)
    file_path = Path.joinpath(base_path, court, file)
    file_url = Path.joinpath(base_url, ident, file)
    
    return Paths(court=court, ident=ident, new_dir=new_dir, file_path=file_path, file_url=file_url)
       
def create_dir(new_dir):
    if not os.path.isdir(new_dir):
        new_dir.mkdir()
    else:
        #print("directory exists \n")
        pass
    
    return None
    
def download_ia_file( file_path, file_url):
    s = requests.session()
    
    if not os.path.isfile(file_path):
        #print('downloading:', file, '\n')
        try:
            r = s.get(file_url)
            print(file_url, r.status_code)
            with open(file_path, 'wb') as f:
                f.write(r.content)
        except:
            print('error downloading:', file, '\n')
            #pass
    else:
        #print('file already exists')
        pass
    return None

def get_files(file):
    Paths = create_paths(file=file)
    court = Paths.court
    new_dir = Paths.new_dir
    ident = Paths.ident
    file_path = Paths.file_path
    file_url = Paths.file_url
    #print(new_dir, file_path)
    #print(court, new_dir, ident, file_path, file_url )
    create_dir(new_dir=new_dir)
    download_ia_file(file_path=file_path, file_url=file_url)
    return None

import concurrent.futures
import threading

def threaded_get_files(file_list):
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        return executor.map(get_files, file_list)


for count, chunk in enumerate(pd.read_csv('/Users/admin/ia_file_list.csv', iterator=False, chunksize=10000)):
    print('chunk: ', count)
    #print(chunk['identifier'])
    file_names = chunk['file_name']
    #[print(file) for file in file_names]
    threaded_get_files(file_names)