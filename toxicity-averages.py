# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 10:31:52 2019

@author: Celia Arsen
@email: celia.arsen@gmail.com

README:

This script walks through a directory with csv files.
The csv files must contain Reddit comments with subreddits and toxicity scores.
Output is two csv files. 
mean-toxicity-by-subr.csv has the average toxicity score for each subreddit accross all files and comments. 
na-comments.csv has the comments that could not be included in the average bc their score was N/A.

This script has not been edited to handle json files as input, or to handle 
very large files that cannot be stored in memory. 

"""
import os
import statistics
import pandas as pd
import csv
import math

DIRECTORY = '/newvolume/OutputData/'
OUTPATH = '/newvolume/OutputAnalysis/'
os.chdir('/newvolume/OutputData')

subreddits_all_scores = {}
na_toxicity = []

def sort_comments(data):
    for index, line in data.iterrows():
        if(not(math.isnan(line['toxicity']))):
            try:
                subreddits_all_scores[line['subreddit']].append(line['toxicity'])
            
            except KeyError:
                subreddits_all_scores[line['subreddit']] = [line['toxicity']]
        
            except Exception:
                pass
        else:
            na_toxicity.append(dict(line))
 
def calculate_mean():
    means = []
    for sub in subreddits_all_scores:
        subdict = {'subreddit':sub, 'score': statistics.mean(subreddits_all_scores[sub]), 'n': len(subreddits_all_scores[sub])}
        means.append(subdict) 
    '''
    print('i am printing means info')
    print(means[0])
    print(means[0].keys())
    '''
    return means

def output_means(means):
    
    keys = means[0].keys()
    with open(OUTPATH+'mean-toxicity-by-subr.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(means)
    output_file.close()

def format_nas():
    nas = []
    for comment in na_toxicity:
        commentDict = {'id': comment['id'], 'body': comment['body'], 'subreddit': comment['subreddit'], 'toxicity': comment['toxicity']}
        nas.append(commentDict)
    return nas
    
def output_nas(nas):
    keys = nas[0].keys()
    with open(OUTPATH+'na-comments.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(nas)
    output_file.close()
    
def main():
    for file in os.listdir(DIRECTORY):
        print(file)
        data = pd.read_csv(file, low_memory=False)
        sort_comments(data)
       
    means = calculate_mean()
    output_means(means)
    nas = format_nas()
    output_nas(nas)
    
main()
