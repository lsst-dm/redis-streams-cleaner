# redis-streams-cleaner

Utilty python script run as a Kubernetes CronJob in Prompt Processing to remove idle Redis clients.  Redis currently has no mechanism to age out old clients which consumes memory over time.  In the future trimming of Redis Streams will be added.