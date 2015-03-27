#!/bin/bash
ssh {remote} 'mkdir -p {fulldir}'
rsync -av ./runfolder/ {remote}:{fulldir}
