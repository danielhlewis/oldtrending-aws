import json
from redis import Redis

if __name__ == "__main__":
  r = Redis()
  redis_queue_name = "process_pages:requests"
  request = {
    'target': '1921-02-05', 
    'source': '/data/images/1921-02-05/1921020501-lu_juggernaut_ver01-sn87090135-00280761801-1.jpg'
  }
  r.rpush(redis_queue_name, json.dumps(request))
  