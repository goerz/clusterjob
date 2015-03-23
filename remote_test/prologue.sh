#!/bin/bash
ssh {remote} 'mkdir -p ~/jobs/remote_test'
rsync -av ./runfolder/ {remote}:~/jobs/remote_test
