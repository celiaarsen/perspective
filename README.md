# perspective

This project compares machine learning models that detect hate speech and other types of abusive and/or problematic behavior in social media.

This repo specifically contains code for scoring the toxicity of Reddit comments using the Perspective API by Google.

Data are from pushshift.io and more information on the Perspective model can be found at https://github.com/conversationai/perspectiveapi

```
+-- one-comment.py                  <-- test to make sure API key and requests to Perspective API are working
+-- rate_comments_perspective.py    <-- iterates thru a directory or json files with online comments, and rates them using Perspective API
+-- select-toxic-subs.py            <-- scans thru a file of zipped Reddit data from pushshift.io, collect comments from specified subreddits, and outputs as json file
+-- toxicity-averages.py            <-- finds average toxicity score for each subreddit accross all files and comments
```


