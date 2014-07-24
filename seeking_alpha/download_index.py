#!/usr/bin/python

""" Downloads and parses article ids from seekingalpha.com.

    Step 1: Read existing article ids from id_dir to ensure no duplicate
            downloads (program terminates when newly downloaded article ids
            are a subset of existing ones).
    Step 2: Iterate all new pages on seekingalpha.com, download one page at
            a time, parse out article ids, and terminate if one of the two
            conditions are met:
            1. New article ids are a subset of existing ones.
            2. There are no new article ids (this should only happen to the
               first download).
            Note there can be overlap between pages, thus we cannot terminate
            when there is overlap of article ids.  We must ensure that all
            article ids on the new page have been seen to terminate.
    Step 3: Write new article ids to a new file to id_dir in the format:
            run_yyyymmddHHMMSS.txt
"""

from datetime import datetime
import argparse
import os

WGET = '/usr/local/bin/wget'
RETRIES = 5
BASE_URL = 'http://seekingalpha.com/analysis/all/all'
WGET_OUTPUT = '/tmp/download_index_tmp_output'

# Program terminates (check failure) if download of any page fails.
TERMINATE_ON_DOWNLOAD_FAILURES = True

# Sanity check that we don't download an infinite number of pages
# (ie, the parser is not working correctly).
MAX_PAGES = 10000
# Sanity check that article id parser is not crazy.
MAX_IDS_PER_PAGE = 55

URL_PATTERN_START = "'/article/"
URL_PATTERN_END = "'"

DATE_TIME_PATTERN = '%Y%m%d%H%M%S'

def download(url, output_file):
  cmd = '%s "%s" -q -O "%s"' % (WGET, url, output_file)
  for i in range(RETRIES):
    if os.path.isfile(output_file):
      os.remove(output_file)
    print 'running command: %s (try %d)' % (cmd, i+1)
    if os.system(cmd) == 0:
      return True
  return False

def extract_ids(id_file):
  with open(id_file, 'r') as fp:
    content = fp.read()
  ids = set()
  start = 0
  while True:
    p = content.find(URL_PATTERN_START, start)
    if p < 0:
      break
    q = content.find(URL_PATTERN_END, p+1)
    assert q > p
    ids.add(content[p+1:q])
    start = q + 1
  print 'extracted %d ids' % len(ids)
  return ids

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--id_dir', required=True)
  args = parser.parse_args()

  id_files = [f for f in os.listdir(args.id_dir) if f.endswith('.txt')]
  old_ids = set()
  for id_file in id_files:
    with open('%s/%s' % (args.id_dir, id_file), 'r') as fp:
      lines = fp.read().splitlines()
    for line in lines:
      old_ids.add(line)
  print 'loaded %d article ids from %d files' % (len(old_ids), len(id_files))

  output_file = '%s/run_%s.txt' % (
      args.id_dir, datetime.now().strftime(DATE_TIME_PATTERN))
  assert not os.path.isfile(output_file)

  new_ids = set()
  for i in range(1, MAX_PAGES+1):
    url = '%s/%d' % (BASE_URL, i)
    if not download(url, WGET_OUTPUT):
      assert not TERMINATE_ON_DOWNLOAD_FAILURES
      continue
    page_ids = extract_ids(WGET_OUTPUT)
    assert len(page_ids) <= MAX_IDS_PER_PAGE
    if len(page_ids) == 0:
      print 'no new id is found'
      break
    if page_ids.issubset(old_ids):
      print 'all new ids have been seen'
      break
    new_ids.update(page_ids.difference(old_ids))
    print 'total new ids: %d' % len(new_ids)

  assert len(new_ids) > 0
  with open(output_file, 'w') as fp:
    for new_id in new_ids:
      print >> fp, new_id

if __name__ == '__main__':
  main()

