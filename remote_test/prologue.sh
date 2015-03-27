#!/bin/bash
ssh {remote} 'mkdir -p {workdir}'
rsync -av ./runfolder/ {remote}:{workdir}
