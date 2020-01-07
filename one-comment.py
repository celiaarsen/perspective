# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 09:46:32 2019

@author: Celia Arsen
@email: celia.arsen@gmail.com

Use this to test to make sure API key and requests to Perspective API are working

Change API_KEY to your own key if you want to test a different key.
Change COMMENT if you want to test different test

"""
from googleapiclient import discovery
import json

API_KEY = 'AIzaSyAmM2CN4J9E1TPVcPS3WMRA8kK9ZKfjqlI'
COMMENT = 'Leave me alone, stupid!'

service = discovery.build('commentanalyzer', 'v1alpha1', developerKey=API_KEY)

analyze_request = {
        'comment': {'text': COMMENT},
	'languages': ['en'],
        'requestedAttributes': {'TOXICITY': {}, 'IDENTITY_ATTACK': {}}
    }

response = service.comments().analyze(body=analyze_request).execute()

print (json.dumps(response, indent=2))
print(response['attributeScores']['TOXICITY']['summaryScore']['value'])
