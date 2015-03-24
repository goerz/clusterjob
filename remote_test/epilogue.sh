rsync -av {remote}:~/jobs/remote_test/ ./runfolder
ssh {remote} 'rm -f ~/jobs/remote_test/*'
ssh {remote} 'rm -f ~/jobs/remote_test/.*'
ssh {remote} 'rmdir ~/jobs/remote_test'
