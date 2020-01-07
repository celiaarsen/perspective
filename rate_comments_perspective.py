# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 15:16:25 2019

@author: Celia Arsen
@email: celia.arsen@gmail.com

README:

This script iterates through a directory containing json files with comments,
and rates them using the Perspective API. 

For each file in the input directory, an output json file is written with the 
comment data and the toxicity scores from the Perspective API. 
For more info on the requested attributes from the Perspective API, see:
https://github.com/conversationai/perspectiveapi/blob/master/2-api/models.md

One output file called 'excluded_comments.json' contains the IDs of any comments
that could not be scored by the Perspective API for any reason.

To use this script, change the following variables as indicated:
    
os.chdir('/your-working-directory/')
DIRECTORY = '/file-path-to-directory-with-json-files/'
OUT_PATH = '/file-path-to-where-you-want-output-files-to-go/'
API_KEY = '/your-Perspective-API-key/'

Look at the function create_out_file and make sure the naming conventions
make sense for the input data you are providing. If your input files do not have
an underscore in their names, or multiple input files have the same prefix before 
an underscore, you should change this function, or change your input file names. 

If you get 429 errors (too many HTTP requests) when running this script,
un-comment line 156
"""

import os
from googleapiclient import discovery
import json
#import time

os.chdir('/newvolume/')
DIRECTORY = '/newvolume/InputData/demographics-subs/'
OUT_PATH = '/newvolume/OutputData/demographics-subs/'
API_KEY = 'YOUR-API-KEY-HERE'
MAX_BATCH_SIZE = 1000

#this constructs a Resource object for interacting with the API
service = discovery.build('commentanalyzer', 'v1alpha1', developerKey=API_KEY)

outfile_name = ''
batch_info = []
batch_location = 0

def create_out_file(file):
    '''MAKE SURE THIS MAKES SENSE FOR THE FORMAT YOUR INPUT DATA ARE IN'''
    global outfile_name 
    outfile_name = '{0}_scored.json'.format(file.split('_')[0])

    print('created output file')

def format_request(text):
    #Set the comment text and request attributes for the API request
    analyze_request = {
        'comment': {'text': text},
        'requestedAttributes': {'TOXICITY': {}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {},
                                'INSULT': {}, 'PROFANITY': {}, 'THREAT': {},'SEXUALLY_EXPLICIT': {}, 
                                'FLIRTATION': {}}
    }
            
    #set the request parameters, but do not execute the request
    request = service.comments().analyze(body=analyze_request)
    return request

def callback(request_id, response, exception): 
    global batch_location
    if exception is not None:
        #print('Error analyzing comment "{0}" '.format(request_id))     
        #write this ID to a file of comments that couldn't be rated properly
        with open(OUT_PATH+'excluded_comments.json', 'a', newline='') as ex:
            json.dump(batch_info[batch_location], ex)
            ex.write('\n')
        batch_location += 1
    else:        
        '''  
        print('Comment "{0}" added with score "{1}"'.format(
                request_id, 
                response['attributeScores']['TOXICITY']['summaryScore']['value']))
        '''
        set_scores(response, request_id)
        batch_location += 1
        
#helper function for callback funtion        
def set_scores(response, request_id):
    global batch_info
    global batch_location

    batch_info[batch_location]['toxicity'] = response['attributeScores']['TOXICITY']['summaryScore']['value']
    batch_info[batch_location]['severe_toxicity'] = response['attributeScores']['SEVERE_TOXICITY']['summaryScore']['value']
    batch_info[batch_location]['identity_attack'] = response['attributeScores']['IDENTITY_ATTACK']['summaryScore']['value']
    batch_info[batch_location]['insult'] = response['attributeScores']['INSULT']['summaryScore']['value']
    batch_info[batch_location]['profanity'] = response['attributeScores']['PROFANITY']['summaryScore']['value']
    batch_info[batch_location]['threat'] = response['attributeScores']['THREAT']['summaryScore']['value']
    batch_info[batch_location]['sexually_explicit'] = response['attributeScores']['SEXUALLY_EXPLICIT']['summaryScore']['value']
    batch_info[batch_location]['flirtation'] = response['attributeScores']['FLIRTATION']['summaryScore']['value']
           
    #write data to output file
    with open(OUT_PATH+outfile_name, 'a+', newline='') as o:
        json.dump(batch_info[batch_location], o)
        o.write('\n')
        
def batch_requests(data):
    #reset global batch info
    global batch_info
    batch_info = []
    #create a new batch request that uses the callback function to provide info about the result of the API call        
    batch = service.new_batch_http_request(callback=callback)

    for line in data:
        #read the json line into a dictionary
        comment = json.loads(line)
        batch_info.append(comment)
        #format the request
        request = format_request(comment['body'])
        #add the request to the batch
        batch.add(request, request_id=comment['id'])   
        
    return batch
            
def main():
    
    for file in os.listdir(DIRECTORY):        
        sanity_checker = 0 
        global batch_location
        batch_location = 0
        print()
        print()
        print(file)
        create_out_file(file)
        #get the data
        with open(DIRECTORY+file, "r", encoding='utf-8', errors='ignore') as f:
            lines = []
            for line in f:
                lines.append(line)
                if len(lines) >= MAX_BATCH_SIZE:
                    sanity_checker += 1
                    batch = batch_requests(lines)
                    batch.execute()
                    print('batched and executed ', sanity_checker*MAX_BATCH_SIZE, 'comments')
                    lines = []
                    batch_location = 0
            if len(lines) > 0:
                batch = batch_requests(lines)
                batch.execute()
                #if you get a 429 error (too many HTTP requests) un-comment the following line
                #time.sleep(1)
                #you can increase or decrease the sleep time depending on your quota limit               
                
            print('finished batching requests for ' + file)       
            print("output file written to "+OUT_PATH+ outfile_name)
        print()

    print("excluded data writted to 'excluded_comments.json'")

main()





