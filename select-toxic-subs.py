# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 10:51:18 2019

@author: Celia Arsen
@email: celia.arsen@gmail.com

README:
    
This script will go through a file of zipped Reddit data from pushshift.io,
collect any comments from the specified subreddits, and output them as a json file.

1. Download the data from pushshift.io by running: 
    wget https://files.pushshift.io/reddit/comments/RC_2019-04.zst
2. Change the 'file' variable to the path of the zipped data
3. Change the 'out_path' variable to the path and name of your new output file
4. Change the Strings in the 'select_subs' list to the subreddits you want to select

""" 

##############################################################################
import zstd
import json

file = '/newvolume/InputData/RC_2019-04.zst'
out_path = '/newvolume/InputData/demographics-subs/demo_2019-04.json'
toxic_subs = ['The_Donald', 'ChapoTrapHouse', 'Ice_Poseidon', 'RoastMe', 'MGTOW']
select_subs = ['blackfellas', 'butchlesbians', 'womenshealth', 'drag', 'blackladies']

gig_counter = 0
toxic_counter = 0
SIXTEEN_MB = 2**24
CHUNKS_PER_GB = 62

with open(file, 'rb') as fh:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(fh) as reader:
        previous_line = ""
        while True:
            gig_counter += 1
            chunk = reader.read(SIXTEEN_MB)
            if(gig_counter%CHUNKS_PER_GB==0):
                print('processed about ', gig_counter/CHUNKS_PER_GB, 'gigs')
                print(toxic_counter, ' comments from select subs')
            if not chunk:
                break

            string_data = chunk.decode('utf-8')
            lines = string_data.split("\n")
            for i, line in enumerate(lines[:-1]):
                if i == 0:
                    line = previous_line + line
                comment = json.loads(line)
                if(comment['subreddit'] in select_subs):
                    toxic_counter += 1
                    with open(out_path, 'a+') as o:
                        json.dump(comment, o)
                        o.write('\n')

            previous_line = lines[-1]

 
