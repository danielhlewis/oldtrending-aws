# Run this file locally to initialize the headlines in the RDS database

import sys
import requests
import json
import mysql.connector
import os
import datetime
import string
from mysql.connector import errorcode
from nltk.sentiment import SentimentIntensityAnalyzer
from chron_json import JSONIterator

import rds_config
#rds settings
rds_host  = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

try:
  cnx = mysql.connector.connect(user=name, password=password, host=rds_host)
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
    print(err)
    sys.exit()
  else:
    print(err)
    sys.exit()
except Exception as err:
    print(err)
    sys.exit()

cursor = cnx.cursor()
try:
  cursor.execute("USE {}".format(db_name))
except mysql.connector.Error as err:
  print("Database {} does not exists.".format(db_name))
  print(err)
  sys.exit()
except Exception as err:
  print(err)
  sys.exit()

def resetDB(connection, cursor):
  print("Reseting Database")
  try:
    cursor.execute('DROP TABLE headline')
    print("Table 'headline' dropped")
  except:
    pass
  cursor.execute('''CREATE TABLE headline (id INTEGER PRIMARY KEY AUTO_INCREMENT,
                 pub_date char(10),
                 page_seq_num int,
                 edition_seq_num varchar(3),
                 batch text,
                 lccn varchar(20),
                 box_x0 real,
                 box_y0 real,
                 box_x1 real,
                 box_y1 real,
                 score real,
                 ocr text,
                 place_of_publication text,
                 name text,
                 publisher text,
                 neg real,
                 neu real,
                 pos real,
                 compound real)''')
  print("Table 'headline' created")
  cursor.execute('''CREATE INDEX headline_pub_date ON headline(pub_date)''')
  print("Index on 'headline.pub_date' created")
  connection.commit()

def download_json(year: str, item_type: str):
  # Pre-extracted headlines are available in a big json file for each year from
  #  the library of congress at well-defined urls, e.g.:
  #    https://news-navigator.labs.loc.gov/prepackaged/1905_headlines.json
  known_types = ['photos', 'illustrations', 'maps', 'comics', 'cartoons', 'headlines', 'ads']
  if item_type not in known_types:
    print('item_type \'{0}\' not in allowable types: {1}'.format(item_type, known_types))
    raise Exception('item_type \'{0}\' not in allowable types: {1}'.format(item_type, known_types))
  filename = '{0}_{1}.json'.format(year, item_type)
  url = 'https://news-navigator.labs.loc.gov/prepackaged/{}'.format(filename)
  local_file = 'data/{}'.format(filename)
  print('Downloading json data from {}...'.format(url))
  r = requests.get(url, stream=True)
  with open(local_file, 'wb') as fd:
    for chunk in r.iter_content(chunk_size=128):
      fd.write(chunk)

if __name__ == '__main__':
  year = datetime.date.today().year - 100
  filename = 'data\{0}_{1}.json'.format(year, 'headlines')
  if not os.path.exists(filename):
    download_json('1921', 'headlines')
  else:
    print('{} found'.format(filename))
  print('Resetting DB')
  resetDB(cnx, cursor)

  with open(filename) as fin:
    sia = SentimentIntensityAnalyzer()
    total_entries = 0
    for o in JSONIterator(fin):
      try:
        j = json.loads(o)
        j['ocr'] = ' '.join(x for x in j['ocr'] if x in string.printable)
        scores = sia.polarity_scores(j['ocr'])
        try:
          add_headline = ("INSERT INTO headline "
                         "(pub_date, page_seq_num, edition_seq_num, batch, lccn, box_x0, box_y0, box_x1, box_y1, score, ocr, place_of_publication, name, publisher, neg, neu, pos, compound) VALUES "
                         "(%(pub_date)s, %(page_seq_num)s, %(edition_seq_num)s, %(batch)s, %(lccn)s, %(box_x0)s, %(box_y0)s, %(box_x1)s, %(box_y1)s, %(score)s, %(ocr)s, %(place_of_publication)s, %(name)s, %(publisher)s, %(neg)s, %(neu)s, %(pos)s, %(compound)s)")
          headline_data = {
            'pub_date': j['pub_date'], 
            'page_seq_num': j['page_seq_num'],
            'edition_seq_num': j['edition_seq_num'],
            'batch': j['batch'],
            'lccn': j['lccn'],
            'box_x0': j['box'][0],
            'box_y0': j['box'][1],
            'box_x1': j['box'][2],
            'box_y1': j['box'][3],
            'score': j['score'],
            'ocr': j['ocr'], 
            'place_of_publication': j['place_of_publication'],
            'name': j['name'],
            'publisher': j['publisher'],
            'neg': scores['neg'],
            'neu': scores['neu'],
            'pos': scores['pos'],
            'compound': scores['compound']
          }
          cursor.execute(add_headline, headline_data)
        except Exception as err:
          print(f"Unexpected {err=}, {type(err)=}: {j['ocr']=}")
      except:
        print("bad object: %s" % o)
        print("Processed %s total entries" % total_entries)
        break
      total_entries += 1
      # # Uncomment to bail out early
      # if total_entries > 100:
      #   break
      if (total_entries % 10000 == 0):
        print("Processed %s entries" % total_entries)
        cnx.commit()
  print("Processed %s entries" % total_entries)
  cnx.commit()
  cnx.close()
