#!/usr/bin/python

import argparse
import os
import time

WGET = '/usr/local/bin/wget'
RETRIES = 5
# TODO: Sleep between requests if we get throttled.
SLEEP_SEC = 0
WGET_OUTPUT = '/tmp/download_articles_tmp_output'

GZIP = '/usr/bin/gzip'
GZ_EXTENSION = '.html.gz'

BASE_URL = 'http://seekingalpha.com'
ID_PREFIX = '/article/'

def download(url, cookie_file, output_file):
  cmd = '%s "%s" -q --load-cookies "%s" -O "%s"' % (
      WGET, url, cookie_file, output_file)
  for i in range(RETRIES):
    if os.path.isfile(output_file):
      os.remove(output_file)
    print 'sleeping for %f sec' % SLEEP_SEC
    time.sleep(SLEEP_SEC)
    print 'running command: %s (try %d)' % (cmd, i+1)
    if os.system(cmd) == 0:
      return True
  return False

def gzip(input_file, output_file):
  cmd = 'gzip -c "%s" > "%s"' % (input_file, output_file)
  print 'running command: %s' % cmd
  return os.system(cmd) == 0

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--id_dir', required=True)
  parser.add_argument('--cookie_file', required=True)
  parser.add_argument('--article_dir', required=True)
  args = parser.parse_args()

  id_files = [f for f in os.listdir(args.id_dir) if f.endswith('.txt')]
  id_map = dict()
  for id_file in id_files:
    with open('%s/%s' % (args.id_dir, id_file), 'r') as fp:
      lines = fp.read().splitlines()
    for line in lines:
      assert line.startswith(ID_PREFIX)
      q = line.find('-', len(ID_PREFIX))
      assert q > len(ID_PREFIX)
      key = line[len(ID_PREFIX):q]
      assert key not in id_map
      id_map[key] = line
  print 'loaded %d article ids from %d files' % (len(id_map), len(id_files))

  article_folders = [f for f in os.listdir(args.article_dir)
                     if os.path.isdir('%s/%s' % (args.article_dir, f))]
  for article_folder in article_folders:
    article_files = [f for f in
                     os.listdir('%s/%s' % (args.article_dir, article_folder))
                     if f.endswith(GZ_EXTENSION)]
    for article_file in article_files:
      article_id = article_file[:article_file.find('.')]
      if article_id in id_map:
        del id_map[article_id]
  print '%d new articles to download' % len(id_map)

  # First folder id for new downloads.
  if len(article_folders) == 0:
    base_folder_id = 0
  else:
    base_folder_id = int(max(article_folders)) + 1

  count, succeeded, failed = 0, 0, 0
  for key, value in id_map.iteritems():
    # We place a max of 1000 article files per folder.
    folder_id = base_folder_id + count/1000
    # Sanity check.  We should never have these many folders in output dir.
    assert folder_id < 10000
    output_dir = '%s/%04d' % (args.article_dir, folder_id)
    if not os.path.isdir(output_dir):
      os.mkdir(output_dir)

    count += 1
    print 'downloading %d/%d files: %s [%d succeeded, %d failed]' % (
        count, len(id_map), key, succeeded, failed)

    url = '%s/%s' % (BASE_URL, value)
    ok = download(url, args.cookie_file, WGET_OUTPUT)
    if not ok:
      failed += 1
      continue
    succeeded += 1

    output_file = '%s/%s%s' % (output_dir, key, GZ_EXTENSION)
    assert gzip(WGET_OUTPUT, output_file)

  print 'processed %d files [%d succeeded, %d failed]' % (
      count, succeeded, failed)

if __name__ == '__main__':
  main()

